#[cfg(feature = "rocksdb")]
mod rocks;

use anyhow::Result;
use clap::Parser;
use rustyline::{
    completion::{extract_word, Completer},
    error::ReadlineError,
    highlight::Highlighter,
    hint::Hinter,
    validate::Validator,
    Helper,
};
use squalodon::{
    lexer::{is_valid_identifier_char, LexerError, SegmentKind, Segmenter},
    storage::{Memory, Storage},
    Connection, Database, Rows,
};
use std::{cell::RefCell, path::PathBuf};
use unicode_width::UnicodeWidthStr;

#[derive(Parser, Debug)]
struct Args {
    /// Filename of the database
    filename: Option<PathBuf>,

    /// Read/process named file
    #[arg(long)]
    init: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    match &args.filename {
        #[cfg(feature = "rocksdb")]
        Some(filename) => {
            let db = rocksdb::TransactionDB::open_default(filename)?;
            run(args, rocks::RocksDB::new(db))
        }
        #[cfg(not(feature = "rocksdb"))]
        Some(_) => anyhow::bail!("RocksDB support is not enabled"),
        None => run(args, Memory::new()),
    }
}

fn run<T: Storage>(args: Args, storage: T) -> Result<()> {
    let db = Database::new(storage)?;
    let conn = db.connect();

    if let Some(init) = args.init {
        let init = std::fs::read_to_string(init)?;
        run_sql(&conn, &init, false)?;
    }

    let mut rl = rustyline::Editor::new()?;
    rl.set_helper(Some(RustylineHelper::new(db.connect())));

    let mut buf = String::new();
    loop {
        let prompt = if buf.is_empty() { "> " } else { ". " };
        let line = match rl.readline(prompt) {
            Ok(line) => line,
            Err(ReadlineError::Eof) => break,
            Err(ReadlineError::Interrupted) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let trimmed_line = line.trim();
        if buf.is_empty() {
            if trimmed_line.is_empty() {
                continue;
            }
            if line.starts_with('.') {
                rl.add_history_entry(&line)?;
                match run_metacommand(&conn, trimmed_line) {
                    Ok(()) => (),
                    Err(e) => eprintln!("{e}"),
                }
                continue;
            }
        }
        buf.push_str(&line);
        if !trimmed_line.ends_with(';') {
            buf.push('\n');
            continue;
        }

        // When the line ends with a semicolon, there are two possibilities:
        // - The semicolon finishes a statement.
        // - The semicolon is inside a string literal.

        if run_sql(&conn, &buf, true)? {
            rl.add_history_entry(&buf)?;
            buf.clear();
        }
    }
    run_sql(&conn, &buf, false)?;

    Ok(())
}

fn run_sql<T: Storage>(conn: &Connection<T>, sql: &str, repl: bool) -> Result<bool> {
    let segmenter = Segmenter::new(sql);
    let mut statements = Vec::new();
    let mut current_statement = String::new();
    for segment in segmenter {
        match segment {
            Ok(segment) => {
                match segment.kind() {
                    SegmentKind::Comment => (),
                    SegmentKind::Operator if segment.slice() == ";" => {
                        // This semicolon finishes a statement.
                        statements.push(std::mem::take(&mut current_statement));
                    }
                    _ => current_statement.push_str(segment.slice()),
                }
            }
            Err((e, remaining)) => {
                if repl && matches!(e, LexerError::UnexpectedEof) {
                    return Ok(false);
                }
                // Let the database handle the parse error.
                current_statement.push_str(remaining);
                break;
            }
        }
    }
    statements.push(current_statement);
    for statement in statements {
        let statement = statement.trim();
        if statement.is_empty() {
            continue;
        }
        match conn.query(statement, []) {
            Ok(rows) => write_table(&mut std::io::stdout().lock(), rows)?,
            Err(e) if repl => {
                eprintln!("{e}");
                return Ok(true);
            }
            Err(e) => eprintln!("{e}"),
        }
    }
    Ok(true)
}

#[derive(Parser, Debug)]
enum Metacommand {
    /// Import data from FILENAME into TABLE
    #[clap(name = ".import")]
    Import {
        /// Use \037 and \036 as column and row separators
        #[arg(long, group = "format")]
        ascii: bool,

        /// Use , and \n as column and row separators
        #[arg(long, group = "format")]
        csv: bool,

        /// Use \t and \n as column and row separators
        #[arg(long, group = "format")]
        tsv: bool,

        /// Skip the first line
        #[arg(long)]
        skip_header: bool,

        filename: PathBuf,
        table: String,
    },

    /// Read input from FILENAME
    #[clap(name = ".read")]
    Read { filename: PathBuf },
}

fn run_metacommand<T: Storage>(conn: &Connection<T>, line: &str) -> Result<()> {
    let parts = std::iter::once("\0").chain(line.split_ascii_whitespace());
    match Metacommand::try_parse_from(parts)? {
        Metacommand::Import {
            ascii,
            csv,
            tsv,
            skip_header,
            filename,
            table,
        } => {
            let mut builder = csv::ReaderBuilder::new();
            if ascii {
                builder
                    .delimiter(b'\x1f')
                    .terminator(csv::Terminator::Any(b'\x1e'));
            } else if csv {
                // Default
            } else if tsv {
                builder.delimiter(b'\t');
            }
            let reader = builder.has_headers(skip_header).from_path(filename)?;
            conn.execute("BEGIN", [])?;
            match insert_into_table(conn, reader, &table) {
                Ok(()) => {
                    conn.execute("COMMIT", [])?;
                }
                Err(e) => {
                    conn.execute("ROLLBACK", [])?;
                    return Err(e);
                }
            }
        }
        Metacommand::Read { filename } => {
            let sql = std::fs::read_to_string(filename)?;
            run_sql(conn, &sql, false)?;
        }
    }
    Ok(())
}

/// Insert rows read from a CSV reader into a table.
///
/// Returns the number of rows inserted.
fn insert_into_table<T: Storage>(
    conn: &Connection<T>,
    mut reader: csv::Reader<std::fs::File>,
    table: &str,
) -> Result<()> {
    use std::fmt::Write;

    let mut statement = None;
    for record in reader.records() {
        let record = record?;
        let statement = match &statement {
            Some(statement) => statement,
            None => {
                let mut sql = "INSERT INTO ".to_owned();
                sql.push_str(&squalodon::lexer::quote(table, '"'));
                sql.push_str(" VALUES (");
                for i in 0..record.len() {
                    if i > 0 {
                        sql.push(',');
                    }
                    write!(&mut sql, "${}", i + 1)?;
                }
                sql.push(')');
                statement.insert(conn.prepare(&sql)?)
            }
        };
        let params: Vec<_> = record.into_iter().map(Into::into).collect();
        statement.execute(params)?;
    }
    Ok(())
}

fn write_table<W: std::io::Write>(out: &mut W, rows: Rows) -> std::io::Result<()> {
    let columns = rows.columns();
    if columns.is_empty() {
        return Ok(());
    }
    let columns = columns.to_vec();
    let mut widths: Vec<_> = columns.iter().map(|column| column.name().width()).collect();
    let mut formatted_rows = Vec::new();
    for row in rows {
        let mut formatted_row = Vec::with_capacity(columns.len());
        for (value, width) in row.columns().iter().zip(widths.iter_mut()) {
            let formatted = value.to_string();
            *width = (*width).max(formatted.width());
            formatted_row.push(formatted);
        }
        formatted_rows.push(formatted_row);
    }
    for (column, width) in columns.into_iter().zip(&widths) {
        write!(out, "{}  ", column.name())?;
        for _ in column.name().width()..*width {
            out.write_all(b" ")?;
        }
    }
    out.write_all(b"\n")?;
    for width in &widths {
        for _ in 0..*width {
            out.write_all(b"-")?;
        }
        out.write_all(b"  ")?;
    }
    out.write_all(b"\n")?;
    for formatted_row in formatted_rows {
        for (formatted, width) in formatted_row.into_iter().zip(&widths) {
            write!(out, "{formatted}  ")?;
            for _ in formatted.width()..*width {
                out.write_all(b" ")?;
            }
        }
        out.write_all(b"\n")?;
    }
    Ok(())
}

struct RustylineHelper<'a, T: Storage> {
    conn: RefCell<Connection<'a, T>>,
}

impl<'a, T: Storage> RustylineHelper<'a, T> {
    fn new(conn: Connection<'a, T>) -> Self {
        Self { conn: conn.into() }
    }
}

impl<T: Storage> Helper for RustylineHelper<'_, T> {}

impl<T: Storage> Completer for RustylineHelper<'_, T> {
    type Candidate = String;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        let (start, word) = extract_word(line, pos, None, |ch| !is_valid_identifier_char(ch));
        let mut candidates = Vec::new();
        let conn = self.conn.borrow_mut();
        let rows = conn
            .query("SELECT keyword FROM squalodon_keywords()", [])
            .unwrap();
        let uppercase_word = word.to_ascii_uppercase();
        for row in rows {
            let keyword: String = row.get(0).unwrap();
            if keyword.starts_with(&uppercase_word) {
                candidates.push(keyword);
            }
        }
        let rows = conn
            .query("SELECT name FROM squalodon_tables()", [])
            .unwrap();
        for row in rows {
            let table_name: String = row.get(0).unwrap();
            if table_name.starts_with(word) {
                candidates.push(table_name);
            }
        }
        Ok((start, candidates))
    }
}

impl<T: Storage> Highlighter for RustylineHelper<'_, T> {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> std::borrow::Cow<'l, str> {
        let mut segmenter = Segmenter::new(line);
        let mut highlighted = String::new();
        for segment in segmenter.by_ref() {
            match segment {
                Ok(segment) => {
                    let color = match segment.kind() {
                        SegmentKind::Literal => {
                            Some("\x1b[33m") // yellow
                        }
                        SegmentKind::Keyword => {
                            Some("\x1b[32m") // green
                        }
                        SegmentKind::Comment => {
                            Some("\x1b[90m") // gray
                        }
                        SegmentKind::Identifier
                        | SegmentKind::Operator
                        | SegmentKind::Whitespace => None,
                    };
                    match color {
                        Some(color) => {
                            highlighted.push_str(color);
                            highlighted.push_str(segment.slice());
                            highlighted.push_str("\x1b[0m");
                        }
                        None => {
                            highlighted.push_str(segment.slice());
                        }
                    }
                }
                Err((_, remaining)) => {
                    highlighted.push_str(remaining);
                    break;
                }
            }
        }
        highlighted.into()
    }

    fn highlight_char(&self, _line: &str, _pos: usize, _forced: bool) -> bool {
        true
    }
}

impl<T: Storage> Hinter for RustylineHelper<'_, T> {
    type Hint = String;
}

impl<T: Storage> Validator for RustylineHelper<'_, T> {}

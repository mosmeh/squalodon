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
    Editor, Helper,
};
use squalodon::{
    lexer::{is_valid_identifier_char, LexerError, SegmentKind, Segmenter},
    storage::{Memory, Storage},
    Connection, Database, Rows,
};
use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};
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
    let db = Database::new(storage);
    let conn = db.connect();
    let mut repl = Repl::new(&conn)?;
    if let Some(init) = args.init {
        repl.process_file(init)?;
    }
    repl.run()
}

struct Repl<'conn, 'db, T: Storage> {
    rl: Editor<RustylineHelper<'conn, 'db, T>, rustyline::history::DefaultHistory>,
    conn: &'conn Connection<'db, T>,
    buf: String,
}

impl<'conn, 'db, T: Storage> Repl<'conn, 'db, T> {
    fn new(conn: &'conn Connection<'db, T>) -> Result<Self> {
        let mut rl = Editor::new()?;
        rl.set_helper(Some(RustylineHelper::new(conn)));
        Ok(Self {
            rl,
            conn,
            buf: String::new(),
        })
    }

    fn run(mut self) -> Result<()> {
        loop {
            let prompt = if self.buf.is_empty() { "> " } else { ". " };
            let line = match self.rl.readline(prompt) {
                Ok(line) => line,
                Err(ReadlineError::Eof) => break,
                Err(ReadlineError::Interrupted) => return Ok(()),
                Err(e) => return Err(e.into()),
            };
            self.run_line(&line)?;
        }
        self.run_sql(false)?; // Finish any incomplete statement
        Ok(())
    }

    fn process_file<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let reader = BufReader::new(File::open(path.as_ref())?);
        for line in reader.lines() {
            self.run_line(&line?)?;
        }
        self.run_sql(false)?; // Finish any incomplete statement
        self.buf.clear();
        Ok(())
    }

    fn run_line(&mut self, line: &str) -> Result<()> {
        let trimmed_line = line.trim();
        if self.buf.is_empty() {
            if trimmed_line.is_empty() {
                return Ok(());
            }
            if line.starts_with('.') {
                self.rl.add_history_entry(line)?;
                match self.run_metacommand(trimmed_line) {
                    Ok(()) => (),
                    Err(e) => eprintln!("{e}"),
                }
                return Ok(());
            }
        }
        self.buf.push_str(line);
        if !trimmed_line.ends_with(';') {
            self.buf.push('\n');
            return Ok(());
        }
        if self.run_sql(true)? {
            self.rl.add_history_entry(&self.buf)?;
            self.buf.clear();
        }
        Ok(())
    }

    fn run_metacommand(&mut self, line: &str) -> Result<()> {
        let parts = std::iter::once(">").chain(line.split_ascii_whitespace());
        let metacommand = Metacommand::try_parse_from(parts)?;
        match metacommand {
            Metacommand::Import {
                ascii,
                csv,
                tsv,
                skip_header,
                escape,
                filename,
                table,
            } => {
                let mut builder = csv::ReaderBuilder::new();
                if ascii {
                    builder.ascii();
                } else if csv {
                    // Default
                } else if tsv {
                    builder.delimiter(b'\t');
                }
                if let Some(escape) = escape {
                    builder.escape(Some(escape.try_into()?));
                }
                let mut reader = builder.has_headers(skip_header).from_path(filename)?;
                let mut inserter = self.conn.inserter(&table)?;
                for record in reader.records() {
                    let params: Vec<_> = record?.into_iter().map(Into::into).collect();
                    inserter.insert(params)?;
                }
                inserter.flush()?;
            }
            Metacommand::Read { filename } => self.process_file(filename)?,
        }
        Ok(())
    }

    fn run_sql(&self, repl: bool) -> Result<bool> {
        let segmenter = Segmenter::new(&self.buf);
        let mut statements = Vec::new();
        let mut current_statement = String::new();
        for segment in segmenter {
            match segment {
                Ok(segment) => {
                    match segment.kind() {
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
            match self.conn.query(&statement, []) {
                Ok(rows) => write_table(&mut std::io::stdout().lock(), rows)?,
                Err(squalodon::Error::NoStatement) => (),
                Err(e) if repl => {
                    eprintln!("{e}");
                    return Ok(true);
                }
                Err(e) => eprintln!("{e}"),
            }
        }
        Ok(true)
    }
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

        /// Escape character
        #[arg(long)]
        escape: Option<char>,

        filename: PathBuf,
        table: String,
    },

    /// Read input from FILENAME
    #[clap(name = ".read")]
    Read { filename: PathBuf },
}

fn write_table<W: std::io::Write>(out: &mut W, rows: Rows) -> std::io::Result<()> {
    const COLUMN_SPACING: usize = 2;

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
    for (i, (column, width)) in columns.into_iter().zip(&widths).enumerate() {
        out.write_all(column.name().as_bytes())?;
        if i == widths.len() - 1 {
            continue;
        }
        for _ in column.name().width()..*width + COLUMN_SPACING {
            out.write_all(b" ")?;
        }
    }
    out.write_all(b"\n")?;
    for (i, width) in widths.iter().enumerate() {
        for _ in 0..*width {
            out.write_all(b"-")?;
        }
        if i == widths.len() - 1 {
            continue;
        }
        for _ in 0..COLUMN_SPACING {
            out.write_all(b" ")?;
        }
    }
    out.write_all(b"\n")?;
    for formatted_row in formatted_rows {
        for (i, (formatted, width)) in formatted_row.into_iter().zip(&widths).enumerate() {
            out.write_all(formatted.as_bytes())?;
            if i == widths.len() - 1 {
                continue;
            }
            for _ in formatted.width()..*width + COLUMN_SPACING {
                out.write_all(b" ")?;
            }
        }
        out.write_all(b"\n")?;
    }
    Ok(())
}

struct RustylineHelper<'conn, 'db, T: Storage> {
    conn: &'conn Connection<'db, T>,
}

impl<'conn, 'db, T: Storage> RustylineHelper<'conn, 'db, T> {
    fn new(conn: &'conn Connection<'db, T>) -> Self {
        Self { conn }
    }
}

impl<T: Storage> Helper for RustylineHelper<'_, '_, T> {}

impl<T: Storage> Completer for RustylineHelper<'_, '_, T> {
    type Candidate = String;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        let (start, word) = extract_word(line, pos, None, |ch| !is_valid_identifier_char(ch));
        let mut candidates = Vec::new();
        let rows = self
            .conn
            .query("SELECT keyword FROM squalodon_keywords()", [])
            .unwrap();
        let uppercase_word = word.to_ascii_uppercase();
        for row in rows {
            let keyword: String = row.get(0).unwrap();
            if keyword.starts_with(&uppercase_word) {
                candidates.push(keyword);
            }
        }
        let rows = self
            .conn
            .query(
                "SELECT name FROM squalodon_tables()
                UNION
                SELECT column_name FROM squalodon_columns()
                UNION
                SELECT name FROM squalodon_functions()",
                [],
            )
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

impl<T: Storage> Highlighter for RustylineHelper<'_, '_, T> {
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

impl<T: Storage> Hinter for RustylineHelper<'_, '_, T> {
    type Hint = String;
}

impl<T: Storage> Validator for RustylineHelper<'_, '_, T> {}

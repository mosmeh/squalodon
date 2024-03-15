use anyhow::Result;
use clap::Parser;
use rustyline::error::ReadlineError;
use squalodon::{
    storage::{KeyValueStore, Memory},
    Database, Rows,
};
use std::{io::Write, path::PathBuf};
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
            run(args, squalodon::storage::RocksDB::new(db))
        }
        #[cfg(not(feature = "rocksdb"))]
        Some(_) => anyhow::bail!("RocksDB support is not enabled"),
        None => run(args, Memory::new()),
    }
}

fn run<S: KeyValueStore>(args: Args, storage: S) -> Result<()> {
    let db = Database::new(storage)?;
    let mut conn = db.connect();
    if let Some(init) = args.init {
        let init = std::fs::read_to_string(init)?;
        conn.execute(&init)?;
    }
    let mut rl = rustyline::DefaultEditor::new()?;
    let mut buf = String::new();
    loop {
        let prompt = if buf.is_empty() { "> " } else { ". " };
        let line = match rl.readline(prompt) {
            Ok(line) => line,
            Err(ReadlineError::Eof | ReadlineError::Interrupted) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        if buf.is_empty() && line.trim().is_empty() {
            continue;
        }
        buf.push_str(&line);
        if !line.trim_end().ends_with(';') {
            buf.push('\n');
            continue;
        }
        rl.add_history_entry(&buf)?;
        let result = conn.query(&buf);
        buf.clear();
        match result {
            Ok(rows) => write_table(&mut std::io::stdout().lock(), rows)?,
            Err(e) => eprintln!("{e}"),
        }
    }
}

fn write_table<W: Write>(out: &mut W, rows: Rows) -> std::io::Result<()> {
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

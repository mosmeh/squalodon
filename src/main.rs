use anyhow::Result;
use rustyline::error::ReadlineError;
use squalodon::{Database, Memory, Storage};

fn main() -> Result<()> {
    let mut db = Database::new(Memory::new())?;
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
        if let Err(e) = query(&mut db, &buf) {
            eprintln!("{e}");
        }
        buf.clear();
    }
}

fn query<S: Storage>(db: &mut Database<S>, sql: &str) -> Result<()> {
    let rows = db.query(sql)?;
    let columns = rows.columns();
    if columns.is_empty() {
        return Ok(());
    }
    let columns = columns.to_vec();
    let mut widths: Vec<_> = columns.iter().map(|column| column.name().len()).collect();
    let mut formatted_rows = Vec::new();
    for row in rows {
        let mut formatted_row = Vec::with_capacity(columns.len());
        for (value, width) in row.columns().iter().zip(widths.iter_mut()) {
            let formatted = value.to_string();
            *width = (*width).max(formatted.len());
            formatted_row.push(formatted);
        }
        formatted_rows.push(formatted_row);
    }
    for (column, width) in columns.into_iter().zip(&widths) {
        print!("{:<width$}  ", column.name());
    }
    println!();
    for width in &widths {
        for _ in 0..*width {
            print!("-");
        }
        print!("  ");
    }
    println!();
    for formatted_row in formatted_rows {
        for (formatted, width) in formatted_row.into_iter().zip(&widths) {
            print!("{formatted:<width$}  ");
        }
        println!();
    }
    Ok(())
}

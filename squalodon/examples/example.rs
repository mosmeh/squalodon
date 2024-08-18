use squalodon::{storage::Memory, Database};

fn main() {
    let db = Database::new(Memory::new());
    let conn = db.connect();

    conn.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
        [],
    )
    .unwrap();

    conn.prepare("INSERT INTO users VALUES ($1, $2, $3)")
        .unwrap()
        .execute((0, "Alice", 42))
        .unwrap();

    {
        let mut inserter = conn.inserter("users").unwrap();
        inserter.insert((1, "Bob", 24)).unwrap();
        inserter.insert((2, "Charlie", 57)).unwrap();
    }

    let rows = conn.query("SELECT name, age FROM users", []).unwrap();
    for row in rows {
        let name: String = row.get(0).unwrap();
        let age: i64 = row.get(1).unwrap();
        println!("name: {name}, age: {age}");
    }
}

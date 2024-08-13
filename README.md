# squalodon

[![build](https://github.com/mosmeh/squalodon/workflows/build/badge.svg)](https://github.com/mosmeh/squalodon/actions)

An embedded relational database management system written from scratch

## Features

- SQL parser
- Planner
- Execution engine
- Join order optimization
- Pluggable storage backends

See the [test cases](squalodon/tests/slt) for the examples of supported features and syntax.

## Usage

```sh
# Start REPL with the in-memory backend
cargo run

# Start REPL with the RocksDB backend
cargo run --features rocksdb -- /path/to/db
```

The following meta-commands are available in the REPL:

```
.import <filename> <table> # Bulk insert data from a CSV/TSV file
.read <filename>           # Execute SQL commands from a file
```

## Testing

The integration tests are powered by [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) which is a Rust port of the [sqllogictest](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) test suite.

Simply run:

```sh
cargo test
```

[tests/slt](squalodon/tests/slt) directory contains the test cases.

## Storage backend

squalodon works with arbitrary transactional key-value stores as its storage backend. The following backends are provided:

- [In-memory](squalodon/src/storage/memory.rs)
- [Blackhole](squalodon/src/storage/blackhole.rs)
- [RocksDB](squalodon-cli/src/rocks.rs)

You can add new backends by implementing the `Storage` trait. See the [RocksDB backend](squalodon-cli/src/rocks.rs) for an example.

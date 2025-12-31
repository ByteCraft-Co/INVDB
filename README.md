# INVDB — Invariant Database

## What is INVDB?
- Embedded database engine written in Rust.
- Focused on correctness, explicit invariants, and corruption detection.
- Real enough to learn from, but not production-ready.

## Features
- Page-based storage with fixed-size pages.
- B-Tree indexing over strongly typed keys.
- Persistent catalog storing tables and schemas.
- Row storage with deterministic encoding and validation.
- Explicit corruption detection across headers, catalog, pages, and rows.

## Non-Goals
- No SQL parser or planner.
- No concurrency control.
- No WAL / crash recovery.
- No networked usage.

## Example Usage
```rust
use invdb::{Db, Schema, Column, ColType, Value};

fn main() -> invdb::InvResult<()> {
    let schema = Schema::new(vec![
        Column { name: "age".into(), ty: ColType::U32, nullable: false },
        Column { name: "name".into(), ty: ColType::String, nullable: true },
    ])?;

    let path = "demo.invdb";
    let mut db = Db::create(path)?;
    db.create_table("users", &schema)?;

    let pk = db.insert_row("users", &vec![Value::U32(42), Value::String("Ada".into())])?;
    db.flush()?;

    let mut reopened = Db::open(path)?;
    let row = reopened.get_row_by_pk("users", pk)?.unwrap();
    assert_eq!(row, vec![Value::U32(42), Value::String("Ada".into())]);
    Ok(())
}
```

## On-Disk Format Stability
- Format is experimental and may change between versions.
- No backward compatibility guarantees across releases.

## Project Status
- Complete for learning, experimentation, and extension.
- Library-first; CLI is intentionally omitted.

## License
- TODO (MIT LICENSE)

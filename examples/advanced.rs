use invdb::{Db, Schema, Column, ColType, Row, Value};
use std::env;

const DEFAULT_TABLE: &str = "users";

fn usage() -> ! {
    eprintln!(
        "Usage:
  advanced init <db_path>
  advanced add-user <db_path> <age:u32> <name:string>
  advanced get-user <db_path> <pk:u32>
  advanced list-users <db_path>

Notes:
  - This example uses a default table named 'users'
  - The schema is: age (U32, not null), name (String, nullable)"
    );
    std::process::exit(2);
}

fn users_schema() -> Result<Schema, Box<dyn std::error::Error>> {
    Ok(Schema::new(vec![
        Column {
            name: "age".to_string(),
            ty: ColType::U32,
            nullable: false,
        },
        Column {
            name: "name".to_string(),
            ty: ColType::String,
            nullable: true,
        },
    ])?)
}

/// Ensure the DB exists and the default table exists.
/// If DB doesn't exist, create it.
/// If table doesn't exist, create it.
fn ensure_db_and_table(db_path: &str) -> Result<Db, Box<dyn std::error::Error>> {
    // Try open first; if it fails, create.
    let mut db = match Db::open(db_path) {
        Ok(db) => db,
        Err(_) => Db::create(db_path)?,
    };

    // Ensure table exists.
    let schema = users_schema()?;
    let existing = db.get_table(DEFAULT_TABLE)?;
    if existing.is_none() {
        db.create_table(DEFAULT_TABLE, &schema)?;
        db.flush()?;
    }

    Ok(db)
}

fn parse_u32(s: &str, name: &'static str) -> u32 {
    s.parse::<u32>().unwrap_or_else(|_| {
        eprintln!("Invalid {name}: expected u32, got '{s}'");
        std::process::exit(2);
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        usage();
    }

    let cmd = args[1].as_str();
    match cmd {
        "init" => {
            if args.len() != 3 {
                usage();
            }
            let db_path = &args[2];

            let mut db = match Db::open(db_path) {
                Ok(db) => db,
                Err(_) => Db::create(db_path)?,
            };

            let schema = users_schema()?;
            if db.get_table(DEFAULT_TABLE)?.is_none() {
                db.create_table(DEFAULT_TABLE, &schema)?;
            }

            db.flush()?;
            println!("Initialized DB at '{db_path}' with table '{DEFAULT_TABLE}'.");
        }

        "add-user" => {
            if args.len() < 5 {
                usage();
            }
            let db_path = &args[2];
            let age = parse_u32(&args[3], "age");
            let name = args[4..].join(" "); // allow spaces in name

            let mut db = ensure_db_and_table(db_path)?;

            let row = Row::from(vec![
                Value::U32(age),
                Value::String(name),
            ]);

            let pk = db.insert_row(DEFAULT_TABLE, &row)?;
            db.flush()?;
            println!("Inserted user with pk={pk}");
        }

        "get-user" => {
            if args.len() != 4 {
                usage();
            }
            let db_path = &args[2];
            let pk = parse_u32(&args[3], "pk");

            let mut db = ensure_db_and_table(db_path)?;

            match db.get_row_by_pk(DEFAULT_TABLE, pk)? {
                Some(row) => println!("pk={pk} row={row:?}"),
                None => println!("pk={pk} not found"),
            }
        }

        "list-users" => {
            if args.len() != 3 {
                usage();
            }
            let db_path = &args[2];

            let mut db = ensure_db_and_table(db_path)?;
            let rows = db.scan_table(DEFAULT_TABLE)?;

            println!("users table: {} rows", rows.len());
            for (pk, row) in rows {
                println!("  pk={pk} row={row:?}");
            }
        }

        _ => usage(),
    }

    Ok(())
}

// Run:
// cargo run --example advanced -- init demo.invdb
// cargo run --example advanced -- add-user demo.invdb 20 User1
// cargo run --example advanced -- add-user demo.invdb 25 User2
// cargo run --example advanced -- list-users demo.invdb
// cargo run --example advanced -- get-user demo.invdb 1


use invdb::{
    Db,
    Schema,
    Column,
    ColType,
    Row,
    Value,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1) Create (or overwrite) a database file
    let mut db = Db::create("example.invdb")?;

    // 2) Define a table schema
    let schema = Schema::new(vec![
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
    ])?;

    // 3) Create a table
    db.create_table("users", &schema)?;

    // 4) Insert some rows
    let pk1 = db.insert_row(
        "users",
        &Row::from(vec![
            Value::U32(20),
            Value::String("User1".to_string()),
        ]),
    )?;

    let pk2 = db.insert_row(
        "users",
        &Row::from(vec![
            Value::U32(25),
            Value::String("User2".to_string()),
        ]),
    )?;

    println!("Inserted users with PKs: {}, {}", pk1, pk2);

    // 5) Query a row by primary key
    if let Some(row) = db.get_row_by_pk("users", pk1)? {
        println!("User {} => {:?}", pk1, row);
    }

    // 6) Scan the whole table
    println!("All users:");
    for (pk, row) in db.scan_table("users")? {
        println!("  pk={} row={:?}", pk, row);
    }

    // 7) Flush changes to disk
    db.flush()?;

    Ok(())
}


// Run:
// cargo run --example basic
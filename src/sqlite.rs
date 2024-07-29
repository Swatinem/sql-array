use anyhow::Result;
use rusqlite::types::Value;
use rusqlite::vtab::array::Array;
use rusqlite::{Connection, ToSql};

use crate::{separated, Row};

pub fn init() -> Result<Connection> {
    let db = Connection::open_in_memory()?;
    rusqlite::vtab::array::load_module(&db)?;
    db.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
        [],
    )?;

    Ok(db)
}

pub fn clear(db: &mut Connection) -> Result<()> {
    db.execute("DELETE FROM test;", ())?;
    Ok(())
}

pub fn insert_one(db: &mut Connection, rows: &[Row]) -> Result<()> {
    let trx = db.transaction()?;
    let mut stmt = trx.prepare("INSERT INTO test (id, name) VALUES (?, ?);")?;
    for row in rows {
        stmt.execute((row.id, &row.name))?;
    }
    stmt.finalize()?;
    trx.commit()?;
    Ok(())
}

pub fn insert_batched(db: &mut Connection, rows: &[Row]) -> Result<()> {
    let trx = db.transaction()?;

    // first: insert chunks using a single prepared query
    const CHUNK_SIZE: usize = 50;
    let mut chunks = rows.chunks_exact(CHUNK_SIZE);
    {
        let chunked_query = format!(
            "INSERT INTO test (id, name) VALUES {};",
            separated("(?, ?)", CHUNK_SIZE)
        );
        let mut chunked_stmt = trx.prepare(&chunked_query)?;
        for chunk in &mut chunks {
            let params = chunk
                .iter()
                .flat_map(|row| [&row.id as &dyn ToSql, &row.name as &dyn ToSql]);
            let params = rusqlite::params_from_iter(params);
            chunked_stmt.execute(params)?;
        }
        chunked_stmt.finalize()?;
    }

    // then: insert the remainder
    {
        let remainder = chunks.remainder();
        let remainder_query = format!(
            "INSERT INTO test (id, name) VALUES {};",
            separated("(?, ?)", remainder.len())
        );
        let params = remainder
            .iter()
            .flat_map(|row| [&row.id as &dyn ToSql, &row.name as &dyn ToSql]);
        let params = rusqlite::params_from_iter(params);
        trx.execute(&remainder_query, params)?;
    }

    trx.commit()?;
    Ok(())
}

pub fn insert_array(db: &mut Connection, rows: &[Row]) -> Result<()> {
    let trx = db.transaction()?;
    let query = "INSERT INTO test (id, name) SELECT * FROM rarray(?) AS a CROSS JOIN rarray(?) AS b ON a.rowid = b.rowid;";

    let ids: Vec<_> = rows.iter().map(|row| Value::from(row.id)).collect();
    let ids = Array::from(ids);

    let names: Vec<_> = rows
        .iter()
        .map(|row| Value::from(row.name.clone()))
        .collect();
    let names = Array::from(names);

    trx.execute(query, (ids, names))?;

    trx.commit()?;
    Ok(())
}

pub fn query_one(db: &Connection, ids: &[u32]) -> Result<Vec<Row>> {
    let query = format!(
        "SELECT id, name from test WHERE id IN ({});",
        separated("?", ids.len())
    );
    let mut stmt = db.prepare(&query)?;
    let params = rusqlite::params_from_iter(ids.iter());
    let rows: Vec<_> = stmt
        .query_map(params, row_from_raw)?
        .collect::<Result<_, _>>()?;
    Ok(rows)
}

pub fn query_array(db: &Connection, ids: &[u32]) -> Result<Vec<Row>> {
    let mut stmt = db.prepare("SELECT id, name from test WHERE id IN rarray(?);")?;
    let ids: Vec<_> = ids.iter().copied().map(Value::from).collect();
    let ids = Array::from(ids);

    let rows: Vec<_> = stmt
        .query_map([ids], row_from_raw)?
        .collect::<Result<_, _>>()?;
    Ok(rows)
}

fn row_from_raw(raw: &rusqlite::Row) -> rusqlite::Result<Row> {
    Ok(Row {
        id: raw.get(0)?,
        name: raw.get(1)?,
    })
}

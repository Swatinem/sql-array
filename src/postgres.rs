use anyhow::Result;
use postgres::fallible_iterator::FallibleIterator as _;
use postgres::types::ToSql;
use postgres::Client;

use crate::Row;

pub fn init() -> Result<Client> {
    let mut client = Client::connect("host=localhost user=postgres", postgres::NoTls)?;

    client
        .batch_execute("CREATE TEMPORARY TABLE test (id INT PRIMARY KEY, name TEXT NOT NULL);")?;

    Ok(client)
}

pub fn clear(db: &mut Client) -> Result<()> {
    db.execute("TRUNCATE test;", &[])?;
    Ok(())
}

pub fn insert_one(db: &mut Client, rows: &[Row]) -> Result<()> {
    let mut trx = db.transaction()?;
    let stmt = trx.prepare("INSERT INTO test (id, name) VALUES ($1, $2);")?;
    for row in rows {
        trx.execute(&stmt, &[&row.id, &row.name])?;
    }
    trx.commit()?;
    Ok(())
}

pub fn insert_batched(db: &mut Client, rows: &[Row]) -> Result<()> {
    let mut trx = db.transaction()?;

    // first: insert chunks using a single prepared query
    const CHUNK_SIZE: usize = 50;
    let mut chunks = rows.chunks_exact(CHUNK_SIZE);
    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(CHUNK_SIZE * 2);
    {
        let chunked_query = format!(
            "INSERT INTO test (id, name) VALUES {};",
            make_placeholders(CHUNK_SIZE, 2)
        );
        let chunked_stmt = trx.prepare(&chunked_query)?;
        for chunk in &mut chunks {
            params.extend(chunk.iter().flat_map(|row| {
                [
                    &row.id as &(dyn ToSql + Sync),
                    &row.name as &(dyn ToSql + Sync),
                ]
            }));
            trx.execute(&chunked_stmt, &params)?;
            params.clear();
        }
    }

    // then: insert the remainder
    {
        let remainder = chunks.remainder();
        let remainder_query = format!(
            "INSERT INTO test (id, name) VALUES {};",
            make_placeholders(remainder.len(), 2)
        );
        params.extend(chunks.remainder().iter().flat_map(|row| {
            [
                &row.id as &(dyn ToSql + Sync),
                &row.name as &(dyn ToSql + Sync),
            ]
        }));
        trx.execute(&remainder_query, &params)?;
        params.clear();
    }

    trx.commit()?;
    Ok(())
}

pub fn insert_array(db: &mut Client, rows: &[Row]) -> Result<()> {
    let mut trx = db.transaction()?;
    let query = "INSERT INTO test (id, name) SELECT * FROM UNNEST($1::INT[], $2::TEXT[]);";

    let ids: Vec<_> = rows.iter().map(|row| row.id).collect();
    let names: Vec<_> = rows.iter().map(|row| row.name.as_str()).collect();

    trx.execute(query, &[&ids, &names])?;

    trx.commit()?;
    Ok(())
}

pub fn query_one(db: &mut Client, ids: &[i32]) -> Result<Vec<Row>> {
    let query = format!(
        "SELECT id, name from test WHERE id IN ({});",
        make_placeholders(ids.len(), 1)
    );

    let mut rows = vec![];
    let mut iter = db.query_raw(&query, ids)?;
    while let Some(raw) = iter.next()? {
        rows.push(row_from_raw(&raw)?);
    }
    rows.sort();
    Ok(rows)
}

pub fn query_array(db: &mut Client, ids: &[i32]) -> Result<Vec<Row>> {
    let query = "SELECT id, name from test WHERE id IN (SELECT * FROM UNNEST($1::INT[]));";

    let mut rows = vec![];
    let mut iter = db.query_raw(query, &[ids])?;
    while let Some(raw) = iter.next()? {
        rows.push(row_from_raw(&raw)?);
    }
    rows.sort();
    Ok(rows)
}

fn row_from_raw(raw: &postgres::Row) -> Result<Row> {
    Ok(Row {
        id: raw.try_get(0)?,
        name: raw.try_get(1)?,
    })
}

pub fn make_placeholders(groups: usize, group_size: usize) -> String {
    use std::fmt::Write;

    let mut s = String::new();
    let mut counter = 1;

    for i in 0..groups {
        if i > 0 {
            s.push_str(", ");
        }

        if group_size == 1 {
            write!(&mut s, "${counter}").unwrap();
            counter += 1;
        } else {
            s.push('(');
            for j in 0..group_size {
                if j > 0 {
                    s.push_str(", ");
                }
                write!(&mut s, "${counter}").unwrap();
                counter += 1;
            }
            s.push(')');
        }
    }

    s
}

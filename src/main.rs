// https://dev.to/forbeslindesay/postgres-unnest-cheat-sheet-for-bulk-operations-1obg
// https://voidstar.tech/sqlite_insert_speed/#carray

use anyhow::Result;
use sql_array::{postgres, sqlite, Row};

fn main() -> Result<()> {
    let rows: Vec<_> = (0..200)
        .map(|id| Row {
            id,
            name: format!("Row {id}"),
        })
        .collect();

    let mut db = sqlite::init()?;

    {
        sqlite::insert_one(&mut db, &rows[..155])?;

        let queried = sqlite::query_one(&db, &[51, 52, 53])?;
        assert_eq!(queried, &rows[51..=53]);
        let queried = sqlite::query_array(&db, &[51, 52, 53])?;
        assert_eq!(queried, &rows[51..=53]);

        sqlite::clear(&mut db)?;
    }

    {
        sqlite::insert_batched(&mut db, &rows[..155])?;

        let queried = sqlite::query_one(&db, &[51, 52, 53])?;
        assert_eq!(queried, &rows[51..=53]);
        let queried = sqlite::query_array(&db, &[51, 52, 53])?;
        assert_eq!(queried, &rows[51..=53]);

        sqlite::clear(&mut db)?;
    }

    {
        sqlite::insert_array(&mut db, &rows[..155])?;

        let queried = sqlite::query_one(&db, &[51, 52, 53])?;
        assert_eq!(queried, &rows[51..=53]);
        let queried = sqlite::query_array(&db, &[51, 52, 53])?;
        assert_eq!(queried, &rows[51..=53]);

        sqlite::clear(&mut db)?;
    }

    let mut db = postgres::init()?;

    {
        postgres::insert_one(&mut db, &rows[..155])?;

        let queried = postgres::query_one(&mut db, &[51, 52, 53])?;
        assert_eq!(queried, &rows[51..=53]);
        let queried = postgres::query_array(&mut db, &[51, 52, 53])?;
        assert_eq!(queried, &rows[51..=53]);

        postgres::clear(&mut db)?;
    }

    {
        postgres::insert_batched(&mut db, &rows[..155])?;

        let queried = postgres::query_one(&mut db, &[51, 52, 53])?;
        assert_eq!(queried, &rows[51..=53]);
        let queried = postgres::query_array(&mut db, &[51, 52, 53])?;
        assert_eq!(queried, &rows[51..=53]);

        postgres::clear(&mut db)?;
    }

    {
        postgres::insert_array(&mut db, &rows[..155])?;

        let queried = postgres::query_one(&mut db, &[51, 52, 53])?;
        assert_eq!(queried, &rows[51..=53]);
        let queried = postgres::query_array(&mut db, &[51, 52, 53])?;
        assert_eq!(queried, &rows[51..=53]);

        postgres::clear(&mut db)?;
    }

    Ok(())
}

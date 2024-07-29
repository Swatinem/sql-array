// DIVAN_SAMPLE_COUNT=10 DIVAN_MIN_TIME=0.5 cargo bench

use divan::Bencher;
use sql_array::Row;

fn main() {
    divan::main();
}

mod sqlite {
    use super::*;

    use sql_array::sqlite;

    #[divan::bench(args=[128, 512, 1024])]
    fn insert_one(bencher: Bencher, n: i32) {
        let rows = make_rows(n);
        bencher
            .counter(n as u64)
            .with_inputs(|| sqlite::init().unwrap())
            .bench_values(|mut db| {
                sqlite::insert_one(&mut db, &rows).unwrap();
                db
            });
    }

    #[divan::bench(args=[128, 512, 1024])]
    fn insert_batched(bencher: Bencher, n: i32) {
        let rows = make_rows(n);
        bencher
            .counter(n as u64)
            .with_inputs(|| sqlite::init().unwrap())
            .bench_values(|mut db| {
                sqlite::insert_batched(&mut db, &rows).unwrap();
                db
            });
    }

    #[divan::bench(args=[128, 512, 1024])]
    fn insert_array(bencher: Bencher, n: i32) {
        let rows = make_rows(n);
        bencher
            .counter(n as u64)
            .with_inputs(|| sqlite::init().unwrap())
            .bench_values(|mut db| {
                sqlite::insert_array(&mut db, &rows).unwrap();
                db
            });
    }

    #[divan::bench]
    fn query_one(bencher: Bencher) {
        let rows = make_rows(55);
        let mut db = sqlite::init().unwrap();
        sqlite::insert_one(&mut db, &rows).unwrap();

        bencher.bench_local(|| sqlite::query_one(&db, &[51, 52, 53]));
    }

    #[divan::bench]
    fn query_array(bencher: Bencher) {
        let rows = make_rows(55);
        let mut db = sqlite::init().unwrap();
        sqlite::insert_one(&mut db, &rows).unwrap();

        bencher.bench_local(|| sqlite::query_array(&db, &[51, 52, 53]));
    }
}

mod postgres {
    use super::*;

    use sql_array::postgres;

    #[divan::bench(args=[128, 512, 1024])]
    fn insert_one(bencher: Bencher, n: i32) {
        let rows = make_rows(n);
        bencher
            .counter(n as u64)
            .with_inputs(|| postgres::init().unwrap())
            .bench_values(|mut db| {
                postgres::insert_one(&mut db, &rows).unwrap();
                db
            });
    }

    #[divan::bench(args=[128, 512, 1024])]
    fn insert_batched(bencher: Bencher, n: i32) {
        let rows = make_rows(n);
        bencher
            .counter(n as u64)
            .with_inputs(|| postgres::init().unwrap())
            .bench_values(|mut db| {
                postgres::insert_batched(&mut db, &rows).unwrap();
                db
            });
    }

    #[divan::bench(args=[128, 512, 1024])]
    fn insert_array(bencher: Bencher, n: i32) {
        let rows = make_rows(n);
        bencher
            .counter(n as u64)
            .with_inputs(|| postgres::init().unwrap())
            .bench_values(|mut db| {
                postgres::insert_array(&mut db, &rows).unwrap();
                db
            });
    }

    #[divan::bench]
    fn query_one(bencher: Bencher) {
        let rows = make_rows(55);
        let mut db = postgres::init().unwrap();
        postgres::insert_one(&mut db, &rows).unwrap();

        bencher.bench_local(|| postgres::query_one(&mut db, &[51, 52, 53]));
    }

    #[divan::bench]
    fn query_array(bencher: Bencher) {
        let rows = make_rows(55);
        let mut db = postgres::init().unwrap();
        postgres::insert_one(&mut db, &rows).unwrap();

        bencher.bench_local(|| postgres::query_array(&mut db, &[51, 52, 53]));
    }
}

fn make_rows(n: i32) -> Vec<Row> {
    (0..n)
        .map(|id| Row {
            id,
            name: format!("Row {id}"),
        })
        .collect()
}

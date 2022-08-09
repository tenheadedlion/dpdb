use criterion::{black_box, criterion_group, criterion_main, Criterion};
use dpdb::Executor;

pub fn write(c: &mut Criterion) {
    let mut executor = Executor::default();
    let _ = executor.execute("reset /media/root_/SLC16/bench.db");
    c.bench_function("write", |b| {
        b.iter(|| executor.execute(black_box("set sdafasdf sdfasdfasdfsadf")))
    });
    let _ = executor.execute("set hay something");
}

pub fn read(c: &mut Criterion) {
    let mut executor = Executor::default();
    c.bench_function("read", |b| {
        b.iter(|| executor.execute(black_box("get needle")))
    });
    let _ = executor.execute("clear");
}

criterion_group!(name = benches; config = Criterion::default(); targets = write, read);
criterion_main!(benches);

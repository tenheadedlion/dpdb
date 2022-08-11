use criterion::{black_box, criterion_group, criterion_main, Criterion};
use dpdb::Executor;

use pprof::criterion::{Output, PProfProfiler};

pub fn write(c: &mut Criterion) {
    let mut executor = Executor::new().unwrap();
    let _ = executor.execute("attach /media/root_/SLC16/bench.db");
    c.bench_function("write", |b| {
        b.iter(|| executor.execute(black_box("set sdafasdf sdfasdfasdfsadf")))
    });
    let _ = executor.execute("set needle hay");
}

pub fn read(c: &mut Criterion) {
    let mut executor = Executor::new().unwrap();
    let _ = executor.execute("attach /media/root_/SLC16/bench.db");
    c.bench_function("read", |b| {
        b.iter(|| {
            let _ = executor.execute(black_box("get needle"));
        })
    });
    let _ = executor.execute("clear");
}

criterion_group!(
    name = benches; 
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None))); 
    targets = write, read);
criterion_main!(benches);

# DPDB

## Usage

Basically a KV database for the moment, to store a value, use the `set key value` syntax, to retrive it, use `get key`, to change the storage file, use `reset path`, but due to security concerns, if the target path exists, the operation will be aborted.

```shell
>> set a 2
4.957024ms
>> set b 2
3.055188ms
>> get b
=> 2
310.024µs
# remove database file
>> clear
200.863µs
>> get a
filesystem failure
>> set a 2
2.175239ms
>> reset /media/h/SLC16/foo.db
2.231462ms
>> get a
=> 2
379.465µs
```

## Benchmark

### 1. Using a compact scheme to store data

Reading data with compact scheme is a lot slower. The flamegraphs shows that `_GI__libc_read` occupied too much CPU time.

```shell
write                   time:   [1.9642 ms 1.9818 ms 2.0020 ms]                   
                        change: [-2.2815% -0.5789% +1.0794%] (p = 0.51 > 0.05)
                        No change in performance detected.
Found 10 outliers among 100 measurements (10.00%)
  3 (3.00%) high mild
  7 (7.00%) high severe

read                    time:   [7.1030 ms 7.1104 ms 7.1181 ms]                 
                        change: [+2299.5% +2306.2% +2312.9%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 3 outliers among 100 measurements (3.00%)
  1 (1.00%) low mild
  2 (2.00%) high mild
```

![](resources/flamegraph_1.svg)

### 0. Store data line by line

```shell
write                   time:   [1.9689 ms 1.9942 ms 2.0240 ms]                   
Found 10 outliers among 100 measurements (10.00%)
  1 (1.00%) high mild
  9 (9.00%) high severe

read                    time:   [294.32 µs 295.02 µs 295.86 µs]                 
```

![](resources/flamegraph_0.svg)

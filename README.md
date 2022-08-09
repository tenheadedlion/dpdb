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

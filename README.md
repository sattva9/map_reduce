# MapReduce Rust

Rust implementation for [6.5840 Lab 1: MapReduce](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)

```shell
make APP=wc sequential-run
make SERVER_ADDR=127.0.0.1:8081 APP=wc distributed-run
make APP=wc compare-output
```

Added implementations for word_count `APP=wc` and indexer `APP=indexer`
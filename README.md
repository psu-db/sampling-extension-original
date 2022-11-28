README
===========

Dependencies
-------------

- gsl
- check

You can install them on Ubuntu through:

```bash
  sudo apt install check libgsl-dev
```

Build
------------

Build using cmake,
```
$ mkdir build
$ cd build
$ cmake ..
$ make
```

Within CMakeLists, the debug flag can be set to false for benchmarking, or true
for debugging. When true, the project is compiled without optimizations, with
debug symbols, and with address sanitizer enabled. Otherwise, the project is
compiled with -O3 and without debug symbols or address sanitizer.

Benchmarking
---------

Benchmark binaries are stored in `bin/benchmarks` and are designed to be
executed with the project root as their working directory. Helper scripts for
doing parameter sweeps, etc., are in `bin/benchmark_scripts`.

To generate a test dataset, use,
```
$ bash bin/utilities/data_proc.sh <number of records> <filename>
```
which will create a data file with a specified number of records into the
specified file. The file will specifically contain two entries per record, the
first is an integer which counts from 0 to (1 - number of records), and the
second is another integer covering the same value range, but shuffled randomly.
This second integer is meant for use as the sampling key, and the first the
sampling value.

Once the data has been generated, the benchmark can be run,
```
$ bin/benchmarks/default_bench <filename> <record_count> <memtable_size> <scale_factor> <selectivity> <memory_levels> <delete_proportion> <max_tombstone_proportion> [insert_batch_proportion]
```
use the same filename and record count parameters that were used to generate
the dataset. Note that `memory_levels` must be at least 1. `delete_proportion`
signifies the number of records that will be deleted after they are inserted,
at some point, and `max_tombstone_proportion` is the maximum proportion of
tombstones on a given level (based on capacity, not record count) that can
exist before a preemptive merge will be triggered. `insert_batch_proportion`
defaults to 0.1 and controls the proportion of the total records that will
be inserted on each benchmarking phase, and for the initial warmup.

Changing the LSM Tree from tiering to leveling can be done by changing the
value of `LSM_LEVELING` within LsmTree.h

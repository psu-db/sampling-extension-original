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

At present, the makefile is not set up to automatically compile the
lockfree_skiplist library, so first compile that,

```
$ cd external/lockfree_skiplist
$ make
$ cd ../..
```

Then build the project. The default make target will build the project in
debug mode and automatically run all unit tests,
```
$ make
```

For benchmarking purposes, build the project in release mode
```
$ make release
```

If the lockfree skiplist library is not installed into the standard library
path, you must also add it to LD_LIBRARY_PATH to run the benchmarks/tests
```
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:external/lockfree_skiplist
```

Benchmarking
---------

Benchmark binaries are stored in `bin/benchmarks` and are designed to
be executed with the project root as their working directory. To generate
a test dataset, use,
```
$ bash bin/utilities/data_proc.sh <number of records> <filename>
```
which will create a data file with a specified number of records into the
specified file. The file will specifically contain two entries per record, the
first is an integer which counts from 0 to 1 - number of records, and the
second is another integer covering the same value range, but shuffled randomly.
This second integer is meant for use as the sampling key, and the first the
sampling value.

Once the data has been generated, the benchmark can be run,
```
$ bin/benchmarks/insert_bench <filename> <record_count> <memtable_size> <scale_factor> <selectivity> <memory_levels>
```
use the same filename and record count parameters that were used to generate
the dataset. Note that `memory_levels` must be at least 1.

The code being benchmarked isn't really instrumented yet. Fine grained
instrumentation can be added directly into `LSMTree::range_sample`, using
`thread_local` variables in LsmTree.h to communicate the results, in the same
manner as is done with the attempt and rejection counters.

Changing the LSM Tree from tiering to leveling can be done by changing
the value of `LSM_LEVELING` within LsmTree.h

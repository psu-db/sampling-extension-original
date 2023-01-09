# README

## Dependencies

- gsl
- check

You can install them on Ubuntu through:

```bash
$ sudo apt install check libgsl-dev
```

The benchmark scripts also rely on the `numactl` utility.
```bash
$ sudo apt install numactl
```

## Build

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

### LSM Tree Configuration
Most properties of the LSM Tree can be adjusted at run-time via constructor arguments,
however there are a few that require compile-time adjustments. First, the `LSM_LEVELING`
variable in `include/lsm/LsmTree.h` can be used to control whether the tree uses a leveling
or a tiering merge policy. The `LSM_REJ_SAMPLE` flag in the same file isn't used currently,
and should be left set as `true`.

Beyond this, there are three branches, each implementing different
functionality. The `master` branch support standard, single threaded IRS with
both memory and disk levels. The `wirs` branch supports single threaded
weighted sampling on memory levels only. The `concurrency` branch supports
concurrent IRS on both memory and disk.

## Benchmarking
Benchmark binaries are stored in `bin/benchmarks` and are designed to be
executed with the project root as their working directory. Helper scripts for
doing parameter sweeps, etc., are in `bin/benchmark_scripts`.

### Generating Data Sets
To generate a test dataset, use,
```bash
$ bash bin/utilities/data_proc.sh <number of records> <filename>
```
which will create a data file with a specified number of records into the
specified file. The file will specifically contain two entries per record, the
first is an integer which counts from 0 to (1 - number of records), and the
second is another integer covering the same value range, but shuffled randomly.
This second integer is meant for use as the sampling key, and the first the
sampling value.

### Shuffling Data Sets
For large datasets, it isn't feasible to use the `shuf` utility due to memory
constraints. In these cases,
```bash
$ bash bin/utilities/eshuff.st <input_filename> <output_filename>
```
can be used to perform an external shuffle on arbitrarily large files.

### Benchmarks
The following benchmarking programs are currently available,

| name | purpose |
|------| ------- |
| `btree_sample` | Loads a BTree with the specified number of records from a file and measures average sampling latency for a given selectivity |
| `static_sample` | Creates a static array with the specified number of records from a file and measures average sampling latency for a given selectivity |
| `lsm_sample` | Creates an LSM Tree with the specified number of records from a file and measures average sampling latency for a given selectivity |
| `lsm_insert` | Measures the average insertion latency into an LSM Tree |
| `lsm_mixed` | Gradually fills up an LSM Tree, measuring the average insertion, deletion, and sampling throughputs at each stage |
| `lsm_bench` | Similar to lsm_mixed, but allows specifying more properties for the LSM Tree |

### Benchmark Scripts
Rather than running the benchmarks directly, the following scripts can be used.
They can be edited to change the experimental parameters for the run, which are
represented as simple Bash arrays,

| name | purpose | 
| ---- | ------- | 
| `sample_latency.sh` | Sweeps over selectivities and uses `btree_sample`, `static_sample`, and `lsm_sample` to measure sampling latencies |
| `insert_tail_latency` | Measures insertion latencies in an LSM Tree and summarizes the percentiles in `tail_latencies.dat` |
| `sample_size_bench.sh` | Uses `{btree,static,lsm}_sample` benchmarks to sweep over selectivity and sample sizes |
| `sample_bench.sh` | Uses `lsm_bench` to sweep over memtable size, scale factor, and max delete proportion|

# README

This is the "original" version of the code associated with SIGMOD 2024 
conference paper: Practical Dynamic Extension for Sampling Indexes, by
Douglas B. Rumbaugh and Dong Xie. It's a bit of a mess, and so a cleaned
up version is available in [another repository](https://github.com/psu-db/sampling-extension),
containing versions of the framework for WSS and WIRS queries. A cleaner implementation of
IRS, including concurrency, [can be found in the repository for our follow-on VLDB paper](https://github.com/psu-db/dynamic-extension). However,
as this is the code-base that was used for the SIGMOD paper itself, we are also making it available in its current state.

The "templated" branches are the ones that were used for generating the figures within the paper. Specifically, the `irs-templated` branch
was used for the IRS figures (including with external storage), the `wirsfr-templated` was used for WSS, `wirs-templated` for WIRS, and
`conc-template` for concurrent IRS. The other branches can be safely ignored except for historical interest.

Note that the extensive use of the term "LSM" to refer to the framework itself is a historical artifact from the early days of the 
project.

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
functionality. The `master` branch supports single-threaded IRS with
both memory and disk levels. The `wirs` branch supports single-threaded
weighted IRS on memory levels only. The `concurrency` branch supports
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
which will create a data file with a specified number of records The file will
specifically contain two entries per record, the first is an integer which
counts from 0 to (1 - number of records), and the second is another integer
covering the same value range, but shuffled randomly. This second integer is
meant for use as the sampling key, and the first the sampling value.

### Shuffling Data Sets
For large datasets, it isn't feasible to use the `shuf` utility due to memory
constraints. In these cases,
```bash
$ bash bin/utilities/eshuf.sh <input_filename> <output_filename>
```
can be used to perform an external shuffle on arbitrarily large files.

### Benchmarks
The following benchmarking programs are currently available,

| name | purpose |
|------| ------- |
| `btree_sample` | Loads an in-memory B+Tree with the specified number of records from a file and measures average sampling latency for a given selectivity |
| `static_sample` | Creates a static array with the specified number of records from a file and measures average sampling latency for a given selectivity |
| `lsm_sample` | Creates an LSM Tree with the specified number of records from a file and measures average sampling latency for a given selectivity |
| `lsm_insert` | Measures the average insertion latency into an LSM Tree |
| `lsm_mixed` | Gradually fills up an LSM Tree, measuring the average insertion, deletion, and sampling throughputs at each stage |
| `lsm_bench` | Similar to lsm_mixed, but allows specifying more parameters of the LSM Tree from the command line, and produces a much more verbose output |

The required arguments for a given benchmark can be seen by running it without
arguments. Each benchmark will write a header to `stderr`, and its normal
output to `stdout`.

### Benchmark Scripts
Rather than running the benchmarks directly, the following scripts can be used.
They can be edited to change the experimental parameters for the run, which are
represented as simple bash variables and arrays,

| name | purpose | 
| ---- | ------- | 
| `sample_latency.sh` | Sweeps over selectivities and uses `btree_sample`, `static_sample`, and `lsm_sample` to measure sampling latencies |
| `insert_tail_latency.sh` | Measures insertion latencies in an LSM Tree and summarizes the percentiles in `tail_latencies.dat` |
| `sample_size_bench.sh` | Uses `{btree,static,lsm}_sample` benchmarks to sweep over selectivity and sample sizes |
| `sample_bench.sh` | Uses `lsm_bench` to sweep over memtable size, scale factor, and max delete proportion|

All of these scripts accept a standardized set of arguments,
```bash
$ bash script.sh <result_dir> <data_file> <numa_node> [record_count]
```
`result_dir` is a directory into which the script will place its output.
The scripts will create this directory automatically, and will error out if the
directory already exists (to avoid accidentally clobbering already collected
data). 

`data_file` contains the records to be inserted, one per line, and should have
the format, 
```
<value> <key> \n
```
for IRS and 
```
<value> <key> <weight> \n
```
for WIRS.

`numa_node` is the NUMA node on which the benchmark programs will be run (using
`numactl(8)`). For a single-node machine, use 0. A list of available nodes can
be found using,
```bash
$ numactl --hardware
```

`record_count` is the number of records in the file. If not specified, it will
be automatically determined using `wc(1)`, but this can take a few moments for
large files.

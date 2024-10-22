# README

This is the "original" version of the code associated with SIGMOD
2024 conference paper: Practical Dynamic Extension for Sampling
Indexes, by Douglas B. Rumbaugh and Dong Xie. It's a bit of
a mess, and so a cleaned up version is available in [another
repository](https://github.com/psu-db/sampling-extension),
containing versions of the framework for WSS and WIRS queries. A
cleaner implementation of IRS, including concurrency,
[can be found in the repository for our follow-on VLDB
paper](https://github.com/psu-db/dynamic-extension). However, as this
is the code-base that was used for the SIGMOD paper itself, we are also
making it available in its current state.

The "templated" branches are the ones that were used for generating
the figures within the paper. Specifically, the `irs-templated` branch
was used for the IRS figures (including with external storage), the
`wirsfr-templated` was used for WSS, `wirs-templated` for WIRS, and
`conc-template` for concurrent IRS. The other branches can be safely
ignored except for historical interest.

Note that the extensive use of the term "LSM" to refer to the framework
itself is a historical artifact from the early days of the project.

## Dependencies

- gsl
- check

You can install them on Ubuntu through:

```bash
$ sudo apt install check libgsl-dev
```

Some scripts also rely on the `numactl` and `rand` utilities.
```bash
$ sudo apt install numactl rand
```

## Build

Build using cmake,
```
$ mkdir build
$ cd build
$ cmake ..
$ make
```

Within CMakeLists, the debug flag can be set to false for benchmarking,
or true for debugging. When true, the project is compiled without
optimizations, with debug symbols, and with address sanitizer
enabled. Otherwise, the project is compiled with -O3 and without debug
symbols or address sanitizer.

### LSM Tree Configuration
Most properties of the LSM Tree can be adjusted at run-time via
constructor arguments, however there are a few that require compile-time
adjustments. First, the `LSM_LEVELING` variable in `include/lsm/LsmTree.h`
can be used to control whether the tree uses a leveling or a tiering merge
policy. The `LSM_REJ_SAMPLE` flag in the same file isn't used currently,
and should be left set as `true`.

Beyond this, there are three branches, each implementing different
functionality. The `master` branch supports single-threaded IRS with
both memory and disk levels. The `wirs` branch supports single-threaded
weighted IRS on memory levels only. The `concurrency` branch supports
concurrent IRS on both memory and disk.

## Benchmarking

Benchmark binaries are stored in `bin/benchmarks` and are designed to
be executed with the project root as their working directory. Helper
scripts for doing parameter sweeps, etc., are in `bin/benchmark_scripts`.

### Generating Data Sets
To generate a test dataset, use,
```bash
$ bash bin/utilities/data_proc.sh <number of records> <filename>
```

which will create a data file with a specified number of records The
file will specifically contain two entries per record, the first is
an integer which counts from 0 to (1 - number of records), and the
second is another integer covering the same value range, but shuffled
randomly. This second integer is meant for use as the sampling key,
and the first the sampling value.


#### Data Set Format
The benchmarks all accept input data as plain text, with the following
formats,
```
<value> <key> \n
```
for IRS and 
```
<value> <key> <weight> \n
```
for WIRS.

The data generator script above will generate files for IRS that follow
this, but you may provide any data you like so long as it follows this
format.

### Shuffling Data Sets
For large datasets, it isn't feasible to use the `shuf` utility due to
memory constraints. In these cases,
```bash
$ bash bin/utilities/eshuf.sh <input_filename> <output_filename>
```
can be used to perform an external shuffle on arbitrarily large files.

### Benchmarks
The following benchmarking programs are currently available,

| name | purpose |
|------| ------- |
| `btree_delete` | Measures delete performance of the B+Tree baseline 
                   and writes the average delete latency to stdout. |
| `btree_throughput` | Measures the average insertion and query throughput 
                       of the B+Tree baseline |
| `isam_throughput` | Measures the average query throughput of a static 
                      ISAM tree | 
| `lsm_delete` | Measures delete performance of the sampling extension
                 structure and writes average delete latency to stdout | 
| `lsm_throughput` | Measures the average insertion and query throughput
                     of the extended sampling structure |
| `static_throughput` | Measures the average query throughput of a static
                        ISAM tree |
| `vldb_irs_bench` | Measures average insertion throughput and query latency
                     using a different benchmarking architecture, to align
                     with the methodology used in our follow-on VLDB 
                     paper |

The required arguments for a given benchmark can be seen by running it
without arguments. Each benchmark will write progress information to
`stderr` and its normal output to `stdout`.

### Running a Benchmark
Each benchmark will require a data file, generated using the above
procedure, and many will also require a query file. The query file
should be formatted using records of the following format,
```
lower_bound_key(int)   upper_bound_key(int)    selectivity(float)
```
where the lower and upper bound keys represent the values to use for the
lower/upper bound of the query, and the selectivity is the selectivity
of the query on the dataset being used. This latter number is used by
some benchmarks to allow specific query selectivities to be benchmarked.

To run a benchmark, first you can generate a dataset using the aforementioned
generator script. We'll use a small set with 10 million records as an
example,
```
$ bash bin/utilities/data_proc.sh 10000000 unif_10m.tsv
```

A query file for use with this uniform 10 million record dataset is
provided in the repository, under `benchmarks/queries/unif_10m_queries.tsv`.

With these files in hand, an example of running the main `lsm_throughput`
benchmark (with the working directory set to the project root) would be,
```
$ bin/benchmarks/lsm_throughput unif_10m.tsv 10000000 12000 6 1000 0.1 \
    0.5 benchmarks/queries/unif_10m_queries.tsv
```

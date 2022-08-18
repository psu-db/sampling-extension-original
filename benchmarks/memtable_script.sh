#!/bin/bash

memtbl_sz=1000000
reccnt=$memtbl_sz

threads=(1 2 4 8 16 32 64)

for thrd in "${threads[@]}"; do
    numactl -m 0 -N 0 benchmarks/memtable_bench 1 $memtbl_sz $reccnt $thrd 0 0 2> /dev/null
done

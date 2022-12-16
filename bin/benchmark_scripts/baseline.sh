#!/bin/bash

if [[ $# <= 1 ]]; then
    echo "Specify results directory" > /dev/stderr
    exit 1
fi

if [[ -d $1 ]]; then
    echo "Refusing to clobber existing results directory" > /dev/stderr
fi

root_dir=$1

mkdir -p root_dir


selectivities=(0.2 0.1 0.05 0.01)

file="/data/dbr4/unif_2b.dat"
count=2000000000

for sel in ${selectivities[@]}; do
    echo -n "$sel " >> ${root_dir}/static.dat
    echo -n "$sel " >> ${root_dir}/btree.dat
    echo -n "$sel " >> ${root_dir}/lsm.dat

    bin/benchmarks/static_bench $file $count $sel >> ${root_dir}/static.dat
    bin/benchmarks/sampling_bench $file $count $sel >> ${root_dir}/lsm.dat
    bin/benchmarks/btree_bench $file $count $sel >> ${root_dir}/btree.dat
done

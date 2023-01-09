#!/bin/bash

if [[ $# < 3 ]]; then
    echo "bench.sh <result_dir> <data_file> <numa_node> [record_count]" > /dev/stderr
    exit 1
fi

result_dir=$1
data_file=$2
numa=$3

if [[ -d $result_dir ]]; then
    echo "Result directory already exists. Refusing to clobber existing data" > /dev/stderr
    exit 1
fi

mkdir -p $result_dir

if [[ ! -f $data_file ]]; then
    echo "Specified data file doesn't exist"
    exit 1
fi

if [[ $# == 4 ]]; then
    reccnt=$4
else
    reccnt=$(wc -l $data_file | cut -d' ' -f1)
fi

selectivities=(.001 .005 .01 .05 .1)
sample_sizes=(1 10 100 1000 10000)
for sel in ${selectivities[@]}; do
    for sz in ${sample_sizes[@]}; do
        printf "%s %s" $sel $sz >> ${result_dir}/static.dat
        printf "%s %s" $sel $sz >> ${result_dir}/btree.dat
        printf "%s %s" $sel $sz >> ${result_dir}/lsm.dat

        numactl -N${numa} bin/benchmarks/static_sample $data_file $reccnt $sel $sz >> ${result_dir}/static.dat
        numactl -N${numa} bin/benchmarks/btree_sample $data_file $reccnt $sel $sz >> ${result_dir}/btree.dat
        numactl -N${numa} bin/benchmarks/lsm_sample $data_file $reccnt $sel $sz >> ${result_dir}/lsm.dat
    done
done


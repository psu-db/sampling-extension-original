#!/bin/bash

if [[ $# -lt 3 ]]; then
    echo "baseline.sh <result_dir> <data_file> <numa_node> [record_count]" > /dev/stderr
    exit 1
fi

result_dir=$1
data_file=$2
numa=$3

if [[ -d $result_dir ]]; then
    echo "Refusing to clobber existing results directory" > /dev/stderr
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

selectivities=(0.2 0.1 0.05 0.01)
sample_sz=1000

for sel in ${selectivities[@]}; do
    echo -n "$sel " >> ${result_dir}/static.dat
    echo -n "$sel " >> ${result_dir}/btree.dat
    echo -n "$sel " >> ${result_dir}/lsm.dat

    numactl -N${numa} bin/benchmarks/static_sample $data_file $reccnt $sel $sample_sz >> ${result_dir}/static.dat
    numactl -N${numa} bin/benchmarks/lsm_sample $data_file $reccnt $sel $sample_sz >> ${result_dir}/lsm.dat
    numactl -N${numa} bin/benchmarks/btree_sample $data_file $reccnt $sel $sample_sz >> ${result_dir}/btree.dat
done

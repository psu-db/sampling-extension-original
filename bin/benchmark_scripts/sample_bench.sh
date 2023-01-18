#!/bin/bash

if [[ $# -lt 3 ]]; then
    echo "sample_bench.sh <result_dir> <data_file> <numa_node> [record_count]" > /dev/stderr
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


memtable=(2000 5000 15000 45000 135000)
selectivity="0.001"
memlevels=1000
scale_factors=(2 4 6 8 10)
del_prop="0.05"
max_ts_props=("0.01" "0.05" "0.1" "0.15")

for mem in ${memtable[@]}; do
    for scale in ${scale_factors[@]}; do
        for del in ${max_ts_props[@]}; do
            numactl -m${numa} -C${numa} bin/benchmarks/lsm_bench "$data_file" "$reccnt" "$mem" "$scale" "$selectivity" "$memlevels" "$del_prop" "$del" | tee ${result_dir}/results_"$mem"_"$scale"_"$del".dat
        done
    done
done

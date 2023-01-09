#!/bin/bash

if [[ $# -lt 3 ]]; then
    echo "insert_tail_latency.sh <result_dir> <data_file> <numa_node> [record_count]" > /dev/stderr
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

memtable_size=20000
memlevels=(100)
scale_factors=(10)

# Level tombstone proportion beyond which compaction will be triggered
max_ts_props=(".1" ".15" ".6") 

# Probability that an inserted record will later be deleted
del_prop="0.5"

for mlvl in ${memlevels[@]}; do
    for sf in ${scale_factors[@]}; do
        for mdel in ${max_ts_props[@]}; do
            fname="${result_dir}/${sf}_${mlvl}_${mdel}.dat"
            numactl -N${numa} bin/benchmarks/lsm_insert "$data_file" "$reccnt" "$memtable_size" "$sf" "$mlvl" "$del_prop" "$mdel" > $fname
            tail_lat=$(bin/utilities/calc_percentile $fname 25 50 75 99)
            printf "%d %d %d %f\t %s\n" $reccnt $mlvl $sf $mdel "$tail_lat" >> "$result_dir/tail_latencies.dat"
        done
    done
done


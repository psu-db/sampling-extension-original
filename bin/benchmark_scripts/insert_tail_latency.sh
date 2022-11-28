#!/bin/bash

result_dir=$1

data_file="unif_100m.dat"
reccnt=100000000

memtable_size=500000
memlevels=(10)
scale_factors=(4)
max_del_props=(".1" ".15" ".6") 
del_prop="0.5"

if [[ -d $result_dir ]]; then
    echo "Specified result directory already exists. Refusing to clobber existing results." > /dev/stderr
    exit 1
fi

mkdir $result_dir

for mlvl in ${memlevels[@]}; do
    for sf in ${scale_factors[@]}; do
        for mdel in ${max_del_props[@]}; do
            fname="${result_dir}/${sf}_${mlvl}_${mdel}.dat"
            bin/benchmarks/insert_bench "$data_file" "$reccnt" "$memtable_size" "$sf" "$mlvl" "$del_prop" "$mdel" > $fname
            tail_lat=$(bin/utilities/calc_percentile $fname 99.99)
            printf "%d %d %d %f\t %f\n" $reccnt $mlvl $sf $mdel $tail_lat >> "$result_dir/tail_latencies.dat"
        done
    done
done


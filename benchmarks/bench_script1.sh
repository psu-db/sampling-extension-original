#!/bin/bash

memtable=( 100000 1000000 )
data_size=( 10000000 100000000 )
scale_factor=( 2 5 )
trials=3

for table in ${memtable[@]}; do
    for data in ${data_size[@]}; do
        for sf in ${scale_factor[@]}; do
            fname="benchmarks/results/${table}_${data}_${sf}.dat"
            echo "record_count tree_height sample_size insert_time rejections attempts cache_misses buffer_proc_time bounds_time walker_time sample_time io_time rej_check_time" > $fname

            echo "Running trials for $data $table $sf"
            for i in $(seq 1 $trials); do
                benchmarks/first_bench "$data" "$table" "$sf" 0 1 >> "$fname"
                rm benchmarks/data/default_bench/*
            done
            echo "Trials done"
        done
    done
done

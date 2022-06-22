#!/bin/bash

memtable=( 10000 50000 100000 150000 )
data_size=( 1000000 50000000 10000000 )
scale_factor=( 2 3 4 5 )
trials=2

for table in ${memtable[@]}; do
    for data in ${data_size[@]}; do
        for sf in ${scale_factor[@]}; do
            fname="benchmarks/results/${table}_${data}_${sf}.dat"
            echo "rejections attempts memtable_time boundary_time walker_time sample_time" > $fname

            echo "Running trials for $data $table $sf"
            for i in $(seq 1 $trials); do
                benchmarks/first_bench "$data" "$table" "$sf" >> "$fname"
                rm benchmarks/data/default_bench/*
            done
            echo "Trials done"
        done
    done
done

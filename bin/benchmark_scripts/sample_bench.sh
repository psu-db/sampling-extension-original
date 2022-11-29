#!/bin/bash

result_dir=$1

memtable=(100000 500000 1000000 2000000)
selectivity="0.5"
data=/data/dbr4/unif_2b.dat
reccnt=2000000000
memlevels=8
scale_factors=(2 4 6)
incoming_deletes="0.1"
deletes=("0.01" "0.05" "0.1" "0.15")

if [[ -d $result_dir ]]; then
    echo "Specified result directory already exists. Refusing to clobber existing results" > /dev/stderr
    exit 1
fi

mkdir -p $result_dir

for mem in ${memtable[@]}; do
    for scale in ${scale_factors[@]}; do
        for del in ${deletes[@]}; do
            bin/benchmarks/default_bench "$data" "$reccnt" "$mem" "$scale" "$selectivity" "$memlevels" "$incoming_deletes" "$del" > ${result_dir}/results_"$mem"_"$scale"_"$del".dat
        done
    done
done

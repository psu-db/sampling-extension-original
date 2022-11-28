#!/bin/bash

fname=$1
bincnt=$2

min_v=$(head -1 $fname)
max_v=$(tail -1 $fname)

binwidth=$(( (max_v - min_v) / (bincnt) ))
echo $binwidth

bin_cnt=0
bin_end=$(( min_v + binwidth ))
while read val; do
    if (( val <= bin_end )); then
        (( bin_cnt++ ))
    else
        printf "%ld %ld\n" $(( bin_end - binwidth )) $bin_cnt
        bin_cnt=0
        bin_end=$(( bin_end + binwidth ))
        while (( bin_end < val )); do
            printf "%ld %ld\n" $(( bin_end - binwidth )) $bin_cnt
            bin_end=$(( bin_end + binwidth ))
        done
        bin_cnt=1
    fi
done <$fname

printf "%ld %ld\n" $(( bin_end - binwidth )) $bin_cnt

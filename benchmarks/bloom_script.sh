#/bin/ksh

sizes=(0 1000 1000000 1000000 10000000 100000000)
hash_funcs=(7 8 9 10 11)

for size in ${sizes[@]}; do
    for funcs in ${hash_funcs[@]}; do
        latency=$(benchmarks/bloom_bench "$size" "$funcs")
        echo "$size" "$funcs" "$latency"
    done
done

memtable_sizes=(10000 100000 1000000)
thread_counts=(1 2 4 8 16 24 32)

for size in ${memtable_sizes[@]}; do
    for threads in ${thread_counts[@]}; do
        benchmarks/unsorted_memtable_bench $size $size $threads 2> /dev/null
    done
done


#!/bin/bash

if [[ $# -lt 2 ]]; then
    echo 'gen_data <count> <filename>'
    exit 1
fi

count=$1
fname=$2

utilities/data_generator "$count" "${fname}_raw"

# larger than memory shuffle via decorate-sort-undecorate with awk, followed by
# line numbering (for simulated external pkey) and cleanup
awk 'BEGIN{srand();} {printf "%0.15f\t%s\n", rand(), $0;}' "${fname}_raw" | sort -n | cut  -f 2- | cat -n | sed 's/^ *//g' > "$fname"

rm "${fname}_raw"

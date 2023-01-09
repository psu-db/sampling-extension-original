#!/bin/bash

if [[ $# -lt 2 ]]; then
    echo 'eshuf <input_filename> <output_filename>'
    exit 1
fi

ifname=$1
ofname=$2

if [[ ! -f "$ifname" ]]; then
    echo 'Invalid input file' > /dev/stderr
    exit 1
fi

if ! touch "$ofname"; then
    echo 'Cannot create output file' >/dev/stderr
    exit 1
fi

rcount=$(wc -l "$ifname" | awk '{ print $1 }')

# if the count is small, it's better to just generate the data in
# one pass
if (( rcount <= 1000000 )); then
    cat "$ifname" | awk 'BEGIN{srand();} {printf "%0.15f\t%s\n", rand(), $0;}' | sort -n | cut  -f 1 --complement - > "$ofname"
    exit 0
fi

# otherwise, we're going to take a more distributed approach

# We want to use a specific number of subfiles, so we need to know how many
# lines to use per file
fcount=100
lines=$(( rcount / fcount ))

# Create a random dir for temporary files--ensuring it is unique and doesn't
# overlap another running datagen job
tmp_dir="TMP$(rand)"
while [[ -d $tmp_prefix ]]; do
    tmp_dir="TMP$(rand)"
done

# If the directory create fails at this point, just 
# bail out an retry manually
if ! mkdir "$tmp_dir"; then
    exit 1
fi

tmp_prefix="$tmp_dir/x"

# Generate all of the data and decorate each line with a random sorting key,
# then split into fcount number of files for parallel sorting
cat "$ifname" | awk 'BEGIN{srand();} {printf "%0.15f\t%s\n", rand(), $0;}' | split -l ${lines} - "${tmp_prefix}"

# Sort each file individually based on the random key
parjobs=10
i=0
for f in $tmp_prefix*; do
    (( i++ ))
    { sort -n $f > "${f}.sorted" && rm $f; } &

    if (( i >= parjobs )); then
        wait
        i=0
    fi
done

wait

# We need to ensure that sort can open all of our files at once, otherwise it
# will need to do many intermediate merge steps and may run out of memory.
batch=$(( fcount + 10 ))

# Merge the sorted files together, strip out the sorting key, add a
# line number as a fake second value, and clean out some whitespace
# for easy processing
sort --batch-size="$batch" -n -m "$tmp_dir"/*.sorted | cut -f 1 --complement -  > "$ofname"

# clean up all the extra temporary files
rm -r "$tmp_dir"

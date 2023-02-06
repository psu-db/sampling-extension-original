
rm -rf plot_files
rm -rf plots

mkdir -p plot_files/
mkdir -p plots/

leveling_dir=$1
tiering_dir=$2

memtable_sf="10"
memtable_dp="0.1"

sf_memtable="15000"
sf_dp="0.1"

dp_memtable="15000"
dp_sf="10"

policies=("leveling" "tiering")
for pol in ${policies[@]}; do
    if [[ $pol == "leveling" ]]; then
        dir=$leveling_dir
    else
        dir=$tiering_dir
    fi

    memtable_sample="plot_files/${pol}_memtable_v_sample.dat"
    memtable_insert="plot_files/${pol}_memtable_v_insert.dat"
    sf_sample="plot_files/${pol}_sf_v_sample.dat"
    sf_insert="plot_files/${pol}_sf_v_insert.dat"
    dp_sample="plot_files/${pol}_dp_v_sample.dat"
    dp_insert="plot_files/${pol}_dp_v_insert.dat"

    for f in ./$dir/*; do
        tmp=$(echo $f | cut -d/ -f3 | cut -d. -f1,2| tr -d [:space:])
        mt=$(echo $tmp | cut -d_ -f2 | tr -d [:space:])
        sf=$(echo $tmp | cut -d_ -f3 | tr -d [:space:])
        dp=$(echo $tmp | cut -d_ -f4 | tr -d [:space:])

        insert=$(tail -1 $f | cut -d' ' -f 11)
        sample=$(tail -1 $f | cut -d' ' -f 12 | cut -f1)

        if [[ $dp == $memtable_dp && $sf == $memtable_sf ]]; then
            printf "%d %d\n" $mt $insert >> $memtable_insert
            printf "%d %d\n" $mt $sample >> $memtable_sample
        fi

        if [[ $dp == $sf_dp && $mt == $sf_memtable ]]; then
            printf "%d %d\n" $sf $insert >> $sf_insert
            printf "%d %d\n" $sf $sample >> $sf_sample
        fi

        if [[ $sf == $dp_sf && $mt == $dp_memtable ]]; then
            printf "%f %d\n" $dp $insert >> $dp_insert
            printf "%f %d\n" $dp $sample >> $dp_sample
        fi
    done

    sort -n $sf_insert > $sf_insert.sorted
    sort -n $sf_sample > $sf_sample.sorted

    sort -n $dp_insert > $dp_insert.sorted
    sort -n $dp_sample > $dp_sample.sorted

    sort -n $memtable_insert > $memtable_insert.sorted
    sort -n $memtable_sample > $memtable_sample.sorted
done

i=0
for f in ./plot_files/leveling_*.sorted; do
    if [[ $f =~ .*_v_sample.* ]]; then
        ylab="Average Sample Latency (ns)"
        if [[ $f =~ .*memtable.* ]]; then
            xlab="MemTable Size (records)"
            title="Average Sample Latency vs. Memtable Size"
        elif [[ $f =~ .*sf.* ]]; then
            xlab="Scale Factor"
            title="Average Sample Latency vs. Scale Factor"
        elif [[ $f =~ .*dp.* ]]; then
            xlab="Maximum Tombstone Proportion"
            title="Average Sample Latency vs. Max. Tombstone Proportion"
        fi
    else
         ylab="Average Insertion Latency (ns)"
        if [[ $f =~ .*memtable.* ]]; then
            xlab="MemTable Size (records)"
            title="Average Insertion Latency vs. Memtable Size"
        elif [[ $f =~ .*sf.* ]]; then
            xlab="Scale Factor"
            title="Average Insertion Latency vs. Scale Factor"
        elif [[ $f =~ .*dp.* ]]; then
            xlab="Maximum Tombstone Proportion"
            title="Average Insertion Latency vs. Max. Tombstone Proportion"
        fi
    fi

    plotfname=./plots/plot_${i}.png
    (( i++ ))

    fstem=$(echo $f | cut -d_ -f3-10)

    f2="plot_files/tiering_"$fstem

    gnuplot << EOF
        set terminal pngcairo size 800,600
        set output "$plotfname"
        set title "$title"
        set xlabel "$xlab"
        set ylabel "$ylab"
        set key outside
        plot "$f"  with linespoints linewidth 1.5 pointsize 2 title "Leveling", \
             "$f2" with linespoints linewidth 1.5 pointsize 2 title "Tiering"
EOF

done

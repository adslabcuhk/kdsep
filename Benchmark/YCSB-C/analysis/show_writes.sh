#!/bin/bash

############ 
# Units: 1 K = 1000, 1 GiB = 2^30 bytes
# Reads consist of: SST reads, blob reads (considering both compaction and Get()), delta OP/GC reads, vLog reads
# Writes consist of: Compaction writes (considering both SST and blob), flush writes, WAL writes, delta OP/GC writes, vLog writes
# 
# comp_w  - (GiB) compaction writes
# flush_w - (GiB) flush writes
# wal_w   - (GiB) WAL writes
# comp_wc - (K)   The number of compaction writes 
# fl_wc   - (K)   The number of flush writes 
# wal_wc  - (K)   The number of wal writes (sync) 
# d_gc_w  - (GiB) Delta GC physical writes 
# d_op_w  - (GiB) Delta OP physical writes 
# d_gc_wc - (K)   The number of delta GC writes 
# g_op_wc - (K)   The number of delta OP writes 
# v_w     - (GiB) vLog writes 
# v_wc    - (K)   The number of vLog writes 
#
# rock_io - (GiB) Total I/O in RocksDB, including SST reads, blob reads, compaction writes, flush writes, WAL writes
# d_rw    - (GiB) Total I/O in deltas, including delta OP/GC reads/writes
# v_rw    - (GiB) Total I/O in vLog, including vLog reads/writes 
# tot_rw  - (GiB) Total I/O = rock_io + d_rw + v_rw

DN=`dirname $0`
source $DN/common.sh

concatFunc "comp_w" "flush_w" "wal_w" "d_gc_w" "d_op_w" "v_w" "tot_w" "tot_wc" "|" "rock_io" "d_rw" "v_rw" "tot_rw" "|" "thpt   " "fname"

files=$*

if [[ "$sortedByTime" == "true" ]]; then
    files=`ls -lht $* | awk '{print $NF;}'`
fi

for file in ${files[@]}; do
    rock_r=`grep "actual.read.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}'`

    comp_w=`grep "rocksdb.compact.write.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}'`
    flush=`grep "rocksdb.flush.write.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}'`
    wal=`grep "rocksdb.wal.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}'`

    rock_wc=`grep "rocksdb.compact.write.count\|rocksdb.flush.write.count\|rocksdb.write.wal" $file | awk 'BEGIN {t=0;} {t+=$NF;} END {print t / 1000;}'`

    rock_io=`echo $rock_r $comp_w $flush $wal | awk '{t=0; for (i=1; i<=NF;i++) if ($1!=0) t+=$i; print t;}'`

    d_gc_w=`grep "dStore GC Physical write bytes" $file | awk 'BEGIN {t=0;} {t+=$7;} END {print t / 1024.0 / 1024.0 / 1024.0;}'`
    d_op_w=`grep "dStore OP Physical write bytes" $file | awk 'BEGIN {t=0;} {t+=$7;} END {print t / 1024.0 / 1024.0 / 1024.0;}'`

#    d_gc_r_cnt=`grep "dStore GC Physical read bytes" $file | awk 'BEGIN {t=0;} {t+=$10;} END {print t / 1000;}'`
#    d_gc_w_cnt=`grep "dStore GC Physical write bytes" $file | awk 'BEGIN {t=0;} {t+=$10;} END {print t / 1000;}'`
#    d_op_r_cnt=`grep "dStore OP Physical read bytes" $file | awk 'BEGIN {t=0;} {t+=$10;} END {print t / 1000;}'`
#    d_op_w_cnt=`grep "dStore OP Physical write bytes" $file | awk 'BEGIN {t=0;} {t+=$10;} END {print t / 1000;}'`

    d_rw=`grep "dStore.*Physical.*bytes" $file | awk 'BEGIN {t=0;} {t+=$7;} END {print t / 1024.0 / 1024.0 / 1024.0;}'`
    d_w_cnt=`grep "dStore.*Physical write bytes" $file | awk 'BEGIN {t=0;} {t+=$10;} END {print t / 1000;}'`
    d_rw_cnt=`grep "dStore.*Physical.*bytes" $file | awk 'BEGIN {t=0;} {t+=$10;} END {print t / 1000;}'`

    thpt=`grep "rocksdb.*workload" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'` 
    if [[ "$thpt" == "0" ]]; then
        loadtime=`grep "Load time" $file | awk 'BEGIN {t=0;} {t=$(NF-1);} END {print t;}'`
        records=`grep "Loading records" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'`
        thpt=`echo "$loadtime $records" | awk '{print $2/($1+0.000001);}'`
    fi

    v_w=`grep "Total disk write" $file | awk 'BEGIN {t=0;} {t+=$NF;} END {print t/1024/1024/1024;}'`
    v_rw=`grep "Total disk" $file | awk 'BEGIN {t=0;} {t+=$NF;} END {print t/1024/1024/1024;}'`
    v_w_cnt=`grep "Flush w/o GC" $file | awk 'BEGIN {t=0;} {t+=$(NF-4);} END {print t/1000;}'`

    tot_w=`echo $comp_w $flush $wal $d_gc_w $d_op_w $v_w | awk '{for (i=1;i<=NF;i++) t+=$i; print t;}'`
    tot_wc=`echo $rock_wc $d_w_cnt $v_w_cnt | awk '{for (i=1;i<=NF;i++) t+=$i; print t;}'`
    tot_rw=`echo $d_rw $v_rw $rock_io | awk '{for (i=1;i<=NF;i++) t+=$i; print t;}'`

    concatFunc "$comp_w" "$flush" "$wal" "$d_gc_w" "$d_op_w" "$v_w" "$tot_w" "$tot_wc" "|" "$rock_io" "$d_rw" "$v_rw" "$tot_rw" "|" "$thpt" "$file"
done

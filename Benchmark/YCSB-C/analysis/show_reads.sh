#!/bin/bash

############ 
# Units: 1 K = 1000, 1 GiB = 2^30 bytes
# Reads consist of: SST reads, blob reads (considering both compaction and Get()), delta OP/GC reads, vLog reads
# Writes consist of: Compaction writes (considering both SST and blob), flush writes, WAL writes, delta OP/GC writes, vLog writes
# 
# sst_r   - (GiB) SST reads 
# sst_rc  - (K)   The number of SST reads 
# blob_r  - (GiB) Blob reads
# blob_rc - (K)   The number of blob reads
# d_gc_r  - (GiB) Delta GC physical reads
# d_op_r  - (GiB) Delta OP physical reads
# d_gc_rc - (K)   The number of delta GC reads
# g_op_rc - (K)   The number of delta OP reads
# v_r     - (GiB) vLog reads
# v_rc    - (K)   The number of vLog reads
#
# rock_io - (GiB) Total I/O in RocksDB, including SST reads, blob reads, compaction writes, flush writes, WAL writes
# d_rw    - (GiB) Total I/O in deltas, including delta OP/GC reads/writes
# v_rw    - (GiB) Total I/O in vLog, including vLog reads/writes 
# tot_rw  - (GiB) Total I/O = rock_io + d_rw + v_rw

DN=`dirname $0`
source $DN/common.sh

#concatFunc "sst_r" "sst_rc" "blob_r" "blob_rc" "d_gc_r" "d_op_r" "d_gc_rc" "d_op_rc" "v_r" "v_rc" "rock_io" "d_rw" "v_rw" "tot_rw" "thpt" "fname"
concatFunc "sst_r" "blob_r" "v_r" "d_gc_r" "d_op_r" "tot_r" "comp_r" "|" "v_rc" "tot_rc" "|" "rock_io" "d_rw" "v_rw" "tot_rw" "|" "thpt   " "fname"

files=$*

if [[ "$sortedByTime" == "true" ]]; then
    files=`ls -lht $* | awk '{print $NF;}'`
fi

for file in ${files[@]}; do
    comp_r=`grep "rocksdb.compact.read.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}'`

    act_sst=`grep "actual.read.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}'`
    act_bl=`grep "actual.blob.read.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}'`
    act_sst=`echo $act_sst $act_bl | awk '{print $1-$2;}'`

    rock_rc=`grep "rocksdb.*last.level.read.count" $file | awk 'BEGIN {t=0;} {t+=$NF;} END {print t / 1000;}'`
#    act_bl_count=`grep "blob.read.count\|blob.read.large.count" $file | awk 'BEGIN {t=0;} {t+=$NF;} END {print t / 1000;}'`

    rock_w=`grep "rocksdb.compact.write.bytes\|rocksdb.flush.write.bytes\|rocksdb.wal.bytes" $file | awk 'BEGIN {t=0;} {t+=$NF;} END {print t / 1024 / 1024 / 1024;}'`

    rock_io=`echo $act_sst $act_bl $rock_w | awk '{t=0; for (i=1; i<=NF;i++) if ($1!=0) t+=$i; print t;}'`

    d_gc_r=`grep "dStore GC Physical read bytes" $file | awk 'BEGIN {t=0;} {t+=$7;} END {print t / 1024.0 / 1024.0 / 1024.0;}'`
    d_op_r=`grep "dStore OP Physical read bytes" $file | awk 'BEGIN {t=0;} {t+=$7;} END {print t / 1024.0 / 1024.0 / 1024.0;}'`

#    d_gc_r_cnt=`grep "dStore GC Physical read bytes" $file | awk 'BEGIN {t=0;} {t+=$10;} END {print t / 1000;}'`
#    d_gc_w_cnt=`grep "dStore GC Physical write bytes" $file | awk 'BEGIN {t=0;} {t+=$10;} END {print t / 1000;}'`
#    d_op_r_cnt=`grep "dStore OP Physical read bytes" $file | awk 'BEGIN {t=0;} {t+=$10;} END {print t / 1000;}'`
#    d_op_w_cnt=`grep "dStore OP Physical write bytes" $file | awk 'BEGIN {t=0;} {t+=$10;} END {print t / 1000;}'`

    d_rw=`grep "dStore.*Physical.*bytes" $file | awk 'BEGIN {t=0;} {t+=$7;} END {print t / 1024.0 / 1024.0 / 1024.0;}'`
    d_r_cnt=`grep "dStore.*Physical read bytes" $file | awk 'BEGIN {t=0;} {t+=$10;} END {print t / 1000;}'`
    d_rw_cnt=`grep "dStore.*Physical.*bytes" $file | awk 'BEGIN {t=0;} {t+=$10;} END {print t / 1000;}'`

    thpt=`grep "rocksdb.*workload" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'` 
    if [[ "$thpt" == "0" ]]; then
        loadtime=`grep "Load time" $file | awk 'BEGIN {t=0;} {t=$(NF-1);} END {print t;}'`
        records=`grep "Loading records" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'`
        thpt=`echo "$loadtime $records" | awk '{print $2/($1+0.000001);}'`
    fi

    v_r=`grep "Total disk read" $file | awk 'BEGIN {t=0;} {t+=$NF;} END {print t/1024/1024/1024;}'`
    v_rw=`grep "Total disk" $file | awk 'BEGIN {t=0;} {t+=$NF;} END {print t/1024/1024/1024;}'`
    v_r_cnt=`grep "GetValueTime" $file | awk 'BEGIN {t=0;} {t+=$(NF-4);} END {print t/1000;}'`

    tot_r=`echo $act_sst $act_bl $d_gc_r $d_op_r $v_r | awk '{for (i=1;i<=NF;i++) t+=$i; print t;}'`
    tot_rc=`echo $rock_rc $d_r_cnt $v_r_cnt | awk '{for (i=1;i<=NF;i++) t+=$i; print t;}'`
    tot_rw=`echo $d_rw $v_rw $rock_io | awk '{for (i=1;i<=NF;i++) t+=$i; print t;}'`

#    concatFunc "$act_sst" "$act_sst_count" "$act_bl" "$act_bl_count" "$d_gc_r" "$d_op_r" "$d_gc_r_cnt" "$d_op_r_cnt" "$v_r" "$v_r_cnt" "$rock_io" "$d_rw" "$v_rw" "$tot_rw" "$thpt" "$file"
    concatFunc "$act_sst" "$act_bl" "$v_r" "$d_gc_r" "$d_op_r" "$tot_r" "$comp_r" "|" "$v_r_cnt" "$tot_rc" "|" "$rock_io" "$d_rw" "$v_rw" "$tot_rw" "|" "$thpt" "$file"
done

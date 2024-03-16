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

concatFunc "ind_rc" "fil_rc" "dat_rc" "tot_rc" "|" "ind_r" "fil_r" "dat_r" "ind_pr" "fil_pr" "dat_pr" "thpt" "file"

files=$*

if [[ "$sortedByTime" == "true" ]]; then
    files=`ls -lht $* | awk '{print $NF;}'`
fi

for file in ${files[@]}; do
    ind_rc=`grep "rocksdb.block.cache.index.add" $file | awk '{t+=$NF;} END {print t/1000000;}'`
    fil_rc=`grep "rocksdb.block.cache.filter.add" $file | awk '{t+=$NF;} END {print t/1000000;}'`
    dat_rc=`grep "rocksdb.block.cache.data.add" $file | awk '{t+=$NF;} END {print t/1000000;}'`
    tot_rc=`echo $ind_rc $fil_rc $dat_rc | awk '{for (i=1;i<=NF;i++) t+=$i; print t;}'`

    ind_r=`grep "rocksdb.block.cache.index.bytes.insert" $file | awk '{t+=$NF;} END {print t/1024/1024/1024;}'`
    fil_r=`grep "rocksdb.block.cache.filter.bytes.insert" $file | awk '{t+=$NF;} END {print t/1024/1024/1024;}'`
    dat_r=`grep "rocksdb.block.cache.data.bytes.insert" $file | awk '{t+=$NF;} END {print t/1024/1024/1024;}'`

    ind_pr=`echo $ind_rc $ind_r | awk '{t+=$2/($1+0.001)*1024*1024/1000000;} END {print t;}'`
    fil_pr=`echo $fil_rc $fil_r | awk '{t+=$2/($1+0.001)*1024*1024/1000000;} END {print t;}'`
    dat_pr=`echo $dat_rc $dat_r | awk '{t+=$2/($1+0.001)*1024*1024/1000000;} END {print t;}'`

    thpt=`grep "rocksdb.*workload" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'` 
    if [[ "$thpt" == "0" ]]; then
        loadtime=`grep "Load time" $file | awk 'BEGIN {t=0;} {t=$(NF-1);} END {print t;}'`
        records=`grep "Loading records" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'`
        thpt=`echo "$loadtime $records" | awk '{print $2/($1+0.000001);}'`
    fi

    concatFunc "$ind_rc" "$fil_rc" "$dat_rc" "$tot_rc" "|" "$ind_r" "$fil_r" "$dat_r" "$ind_pr" "$fil_pr" "$dat_pr" "$thpt" "$file"
done

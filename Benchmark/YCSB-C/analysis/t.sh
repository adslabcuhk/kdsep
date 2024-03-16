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

t=${*}

for file in $t; do
    echo "hi $file"
done

#if [[ "$sortedByTime" == "true" ]]; then
    t2=`ls -lht ${t[*]} | awk '{print $NF;}'`
    for file in $t2; do
        echo "hi2 $file"
    done
    
#fi

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

func() {
    level=$1
    fn=$2
    lvl_cnt=`grep "Level $level read latency" -A 1 $fn | tail -n 1 | awk 'BEGIN {t=0;} {t = $2 / 1000;} END {print t;}'`
    echo $lvl_cnt
}

concatFunc lv0_rc lv1_rc lv2_rc lv3_rc lv4_rc lv5_rc lv6_rc sst_rc blb_rc v_rc tot_rc thpt file

for file in $*; do

    lv0_rc=`func 0 $file`
    lv1_rc=`func 1 $file`
    lv2_rc=`func 2 $file`
    lv3_rc=`func 3 $file`
    lv4_rc=`func 4 $file`
    lv5_rc=`func 5 $file`
    lv6_rc=`func 6 $file`

    thpt=`grep "rocksdb.*workload" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'` 
    if [[ "$thpt" == "0" ]]; then
        loadtime=`grep "Load time" $file | awk 'BEGIN {t=0;} {t=$(NF-1);} END {print t;}'`
        records=`grep "Loading records" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'`
        thpt=`echo "$loadtime $records" | awk '{print $2/($1+0.000001);}'`
    fi

    sst_rc=`echo $lv0_rc $lv1_rc $lv2_rc $lv3_rc $lv4_rc $lv5_rc $lv6_rc | awk '{for (i=1;i<=NF;i++) t+=$i; print t;}'`
    blb_rc=`grep "rocksdb.blobdb.blob.file.read.micros" $file | awk 'BEGIN {t=0;} {t=$(NF-3);} END {print t / 1000;}'`
    v_rc=`grep "GetValueTime" $file | awk 'BEGIN {t=0;} {t+=$(NF-4);} END {print t/1000;}'`

    tot_rc=`echo $sst_rc $blb_rc $v_rc | awk '{for (i=1;i<=NF;i++) t+=$i; print t;}'`

    concatFunc $lv0_rc $lv1_rc $lv2_rc $lv3_rc $lv4_rc $lv5_rc $lv6_rc $sst_rc $blb_rc $v_rc $tot_rc $thpt $file
done

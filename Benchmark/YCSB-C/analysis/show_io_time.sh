#!/bin/bash

############ 
# Units: seconds 
# 
# roc_r   - RocksDB reads
# roc_w   - RocksDB writes (not including fsync) 
# roc_syn - RocksDB fsync 
# d_gc_r  - Delta GC reads 
# d_gc_w  - Delta GC writes 
# d_op_r  - Delta OP reads 
# d_gc_w  - Delta OP writes 
# v_r     - vLog reads
# v_w     - vLog writes 
#
# roc_io  - RocksDB I/O (= roc_r + roc_w + roc_syn) 
# d_io_t  - Delta I/O (= d_gc_r + d_gc_w + d_op_r + d_op_w) 
# v_io_t  - vLog I/O (= v_r + v_w) 
# tot     - Total I/O time (= roc_io + d_io_t + v_io_t) 
# act_t   - Actual run time 

DN=`dirname $0`
source $DN/common.sh

#concatFunc "roc_r" "roc_w" "roc_syn" "d_gc_r" "d_gc_w" "d_op_r" "d_op_w" "v_r" "v_w" "roc_io" "d_io_t" "v_io_t" "tot" "act_t" "thpt" "fname"
concatFunc "sst_r" "blb_r" "d_gc_r" "d_gc_w" "d_op_r" "d_op_w" "v_r" "v_w" "roc_io" "d_io_t" "v_io_t" "tot" "act_t" "thpt" "fname"

for file in $*; do
    fname=$file

    comp_r=`grep "rocksdb.compact.read.nanos" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1000000000;}'`
    comp_w=`grep "rocksdb.compact.write.nanos" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1000000000;}'`

#    roc_r=`grep "rocksdb.file.reader.read.micros" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1000000;}'`

# Real SST read I/O time
    sst_r=`grep "rocksdb.sst.read.micros" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1000000;}'`

# Real blob read I/O time
    blb_r=`grep "rocksdb.blobdb.blob.file.read.micros" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1000000;}'`

#    roc_w=`grep "file_write_micros" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1000000;}'`
#    roc_syn=`grep "file_fsync_micros" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1000000;}'`


    d_gc_r=`grep "gc read" $file | awk 'BEGIN {t=0;} {t=$5;} END {print t / 1000000;}'`
    d_gc_w=`grep "gc write" $file | awk 'BEGIN {t=0;} {t=$5;} END {print t / 1000000;}'`
    d_op_r=`grep "worker-get-file-io" $file | awk 'BEGIN {t=0;} {t=$3;} END {print t / 1000000;}'`
    d_op_w=`grep "worker-put-file-write" $file | awk 'BEGIN {t=0;} {t=$3;} END {print t / 1000000;}'`

    thpt=`grep "rocksdb.*workload" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'` 
    if [[ "$thpt" == "0" ]]; then
        loadtime=`grep "Load time" $file | awk 'BEGIN {t=0;} {t=$(NF-1);} END {print t;}'`
	act_t=$loadtime
        records=`grep "Loading records" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'`
        thpt=`echo "$loadtime $records" | awk '{print $2/($1+0.000001);}'`
    else
	act_t=`grep "run time" $file | sed 's/us//g' | awk 'BEGIN {t=0;} {t=$NF/1000000;} END {print t;}'`
    fi

    v_r=`grep "GetValueTime" $file | awk 'BEGIN {t=0;} {t+=$3;} END {print t/1000000;}'`
    v_w=`grep "Flush w/o GC" $file | awk 'BEGIN {t=0;} {t+=$6;} END {print t/1000000;}'`

    roc_io=`echo $sst_r $blb_r $roc_w $roc_syn | awk '{t=0; for (i=1; i<=NF;i++) if ($1!=0) t+=$i; print t;}'`
    d_io_t=`echo $d_gc_r $d_gc_w $d_op_r $d_op_w | awk '{t=0; for (i=1; i<=NF;i++) if ($1!=0) t+=$i; print t;}'`
    v_io_t=`echo $v_r $v_w | awk '{for (i=1;i<=NF;i++) t+=$i; print t;}'`
    tot=`echo $roc_io $d_io_t $v_io_t | awk '{for (i=1;i<=NF;i++) t+=$i; print t;}'`

#    concatFunc "$sst_r" "$blb_r" "$roc_w" "$roc_syn" "$d_gc_r" "$d_gc_w" "$d_op_r" "$d_op_w" "$v_r" "$v_w" "$roc_io" "$d_io_t" "$v_io_t" "$tot" "$act_t" "$thpt" "$fname"
    concatFunc "$sst_r" "$blb_r" "$d_gc_r" "$d_gc_w" "$d_op_r" "$d_op_w" "$v_r" "$v_w" "$roc_io" "$d_io_t" "$v_io_t" "$tot" "$act_t" "$thpt" "$fname"
done

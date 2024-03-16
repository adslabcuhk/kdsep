#!/bin/bash

DN=`dirname $0`
source $DN/common.sh

#concatFunc "rd_rock" "r_blob" "actual" "act_bl" "comp_w" "flush" "wal" "rock_io" "sst_sz" "rss_gb" "cache-g" "d_rw" "br_sz" "v_rw" "tot_rw" "thpt" "fname"
concatFunc "sst_lr" "blb_lr" "sst_sz" "rss" "c_sz" "thpt" "file"

files=$*

if [[ "$sortedByTime" == "true" ]]; then
    files=`ls -lht $* | awk '{print $NF;}'`
fi

for file in ${files[@]}; do
    rd_rock=`grep "rocksdb.last.level.read.bytes\|rocksdb.non.last.level.read.bytes" $file | awk 'BEGIN {t=0;} {t+=$NF;} END {print t / 1024 / 1024 / 1024;}'`
    blb_lr=`grep "rocksdb.blobdb.blob.file.bytes.read " $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}'`
    sst_lr=`echo $rd_rock $blb_lr | awk '{print $1-$2;}'` 
#    actual=`grep "actual.read.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}'`
#    act_bl=`grep "actual.blob.read.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}'`
#    comp_w=`grep "rocksdb.compact.write.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}'`
#    flush=`grep "rocksdb.flush.write.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}'`
#    wal=`grep "rocksdb.wal.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}'`
#    rock_io=`echo $actual $comp_w $flush $wal | awk '{t=0; for (i=1; i<=NF;i++) if ($1!=0) t+=$i; print t;}'`

    sst_sz=`grep "sst, num" $file | awk 'BEGIN {t=0;} {t=$1;} END {print t / 1024.0;}'`
    rss=`grep "resident" $file | awk 'BEGIN {t=0;} {t=$(NF-1);} END {print t;}'`
#    d_rw=`grep "dStore.*Physical.*bytes" $file | awk 'BEGIN {t=0;} {t+=$7;} END {print t / 1024.0 / 1024.0 / 1024.0;}'`
#    deltaSize=`grep "MiB delta" $file | awk 'BEGIN {t=1000000000;} {if ($1<t) t=$1;} END {print t / 1024.0;}'`
#
#    br_count=`grep "rocksdb.blobdb.blob.file.read.micros" $file | awk 'BEGIN {t=0;} {t=$(NF-3);} END {print t;}'`
#    br_sz=`echo "$act_bl $br_count" | awk '{print $1/($2+0.01)*1024*1024*1024;}'`

    c_sz=`grep "GetUsage()" $file | awk '{t+=$NF;} END {print t/1024/1024/1024;}'`

    thpt=`grep "rocksdb.*workload" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'` 
    if [[ "$thpt" == "0" ]]; then
        loadtime=`grep "Load time" $file | awk 'BEGIN {t=0;} {t=$(NF-1);} END {print t;}'`
        records=`grep "Loading records" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'`
        thpt=`echo "$loadtime $records" | awk '{print $2/($1+0.000001);}'`
    fi

#    v_rw=`grep "Disk     0" $file | awk 'BEGIN {t=0;} {t+=$NF + $(NF-2);} END {print t/1024/1024/1024;}'`
#    tot_rw=`echo $d_rw $v_rw $rock_io | awk '{for (i=1;i<=NF;i++) t+=$i; print t;}'`
    concatFunc "$sst_lr" "$blb_lr" "$sst_sz" "$rss" "$c_sz" "$thpt" "$file"
done

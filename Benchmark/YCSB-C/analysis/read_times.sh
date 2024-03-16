#!/bin/bash

#echo "r_lsm"$'\t'"r_blob"$'\t'"actual"$'\t'"act_bl"$'\t'"sst_sz"$'\t'"rss_gb"$'\t'"cache-g"$'\t'"d_read"$'\t'"d_size"$'\t'"thpt"$'\t'"fname"
echo "r_time"$'\t'"br_time"$'\t'"comp_t"$'\t'"r_avg"$'\t'"br_avg"$'\t'"r_cnt"$'\t'"br_cnt"$'\t'"thpt"$'\t'"fname"

for file in $*; do
#    num1=`grep "rocksdb.last.level.read.bytes" $file | awk '{print $NF;}'`
#    num2=`grep "rocksdb.non.last.level.read.bytes" $file | awk '{print $NF;}'`
#    actual=`grep "actual.read.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}' | cut -c1-7`
#    actual_blob=`grep "actual.blob.read.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}' | cut -c1-7`
#    r_blob=`grep "rocksdb.blobdb.blob.file.bytes.read" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}' | cut -c1-7`
    get_time=`grep "rocksdb.read.block.get.micros" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1000000;}' | cut -c1-7`
    blob_read_time=`grep "rocksdb.blobdb.blob.file.read.micros" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1000000;}' | cut -c1-7`
    compact_time=`grep "rocksdb.compaction.times.micros" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1000000;}' | cut -c1-7`
    r_avg=`grep "rocksdb.read.block.get.micros" $file | awk 'BEGIN {t=0;} {t=$NF / ($(NF-3)+0.001);} END {print t;}' | cut -c1-7`
    br_avg=`grep "rocksdb.blobdb.blob.file.read.micros" $file | awk 'BEGIN {t=0;} {t=$NF / ($(NF-3)+0.001);} END {print t;}' | cut -c1-7`
    r_count=`grep "rocksdb.read.block.get.micros" $file | awk 'BEGIN {t=0;} {t=$(NF-3);} END {print t / 1000000;}' | cut -c1-7`
    br_count=`grep "rocksdb.blobdb.blob.file.read.micros" $file | awk 'BEGIN {t=0;} {t=$(NF-3);} END {print t / 1000000;}' | cut -c1-7`
    thpt=`grep "workload" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}' | cut -c1-7` 
    echo "$get_time"$'\t'"$blob_read_time"$'\t'"$compact_time"$'\t'"$r_avg"$'\t'"$br_avg"$'\t'"$r_count"$'\t'"$br_count"$'\t'"$thpt"$'\t'"$file"
done

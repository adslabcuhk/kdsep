#!/bin/bash

for fname in $*; do
    echo $fname
    if [[ ! -f $fname ]]; then
        echo "continue 0"
        continue
    fi
    pattern=`echo $fname | grep -o "cache[0-9]*_blobcache[0-9]*"`
    if [[ "$pattern" == "" ]]; then
        pattern=`echo $fname | grep -o "cache[0-9]*"`
        if [[ "$pattern" == "" ]]; then
            echo "continue 1"
            continue
        fi
    fi
    pattern2=`echo $fname | grep -o "\-fc[0-9]*-fl[0-9]*_c"`
    if [[ "$pattern2" == "" ]]; then
        echo "continue 2"
        continue
    fi
    if [[ `echo $fname | grep "totcache" | wc -l` -eq 1 ]]; then
        echo "continue 3"
        continue
    fi
    totcache=`echo $pattern | sed "s/_/ /g" | sed "s/blobcache/ /g" | sed "s/cache/ /g" | awk '{s=0; for (i=1;i<=NF;i++) {s+=$i;} print s;}'`
    nfname=`echo $fname | sed "s/$pattern/${pattern}_totcache${totcache}/g" | sed "s/blockSize4096_//g" | sed "s/gcT2-workerT12-BatchSize-2000-//g" | sed "s/$pattern2/c/g"`
    echo $nfname
    if [[ -f $nfname ]]; then
        nfname=`echo $nfname | sed "s/Round-1/Round-2/g"`
        if [[ -f $nfname ]]; then
            echo "exist $nfname"
            exit
        fi
    fi
    mv $fname $nfname
done

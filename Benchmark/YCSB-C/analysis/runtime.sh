#!/bin/bash

DN=`dirname $0`
source $DN/common.sh

concatFunc "runtime" "file" 

files=$*

if [[ "$sortedByTime" == "true" ]]; then
    files=`ls -lht $* | awk '{print $NF;}'`
fi

for file in ${files[@]}; do
    time1=$(grep -A 1 "time 1" $file | tail -n 1)
    time2=$(grep -A 1 "time 2" $file | tail -n 1)
    if [[ $time1 == "" ]]; then
        t=0
    else
        t=$( echo $(( $time2 - $time1 )) | awk 'BEGIN {t=0;} {t=$1;} END {print t / 1000000;}')
    fi
    concatFunc $t $file 
done

#!/bin/bash

while [[ 1 -eq 1 ]]; do
    echo "$(date) ------- $(cat /sys/fs/cgroup/zz2/memory.stat | grep -w "file" | awk '{print $2 / 1024 / 1024 " MiB";}')"
    sleep 5 
done

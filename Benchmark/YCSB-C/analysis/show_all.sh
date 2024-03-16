#!/bin/bash

DR=`dirname $0`
${DR}/calculate_read_bytes.sh $*
echo ""
${DR}/show_reads.sh $*
echo ""
${DR}/show_writes.sh $*
echo ""
${DR}/show_cache.sh $*
echo ""
${DR}/show_counts.sh $*
echo ""
${DR}/show_io_time.sh $*

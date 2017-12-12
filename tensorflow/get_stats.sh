#!/bin/bash


# Get the CPU utitlization
while [ true ]
do
  sleep 10
  top | grep python >> cpu_sync &
done 
# sar >> cpu_stats_sync_output &
# sar -r 10 200 >> mem_stats_sync_output &
# sar -n  TCP 10 200 >> net_stats_sync_output &

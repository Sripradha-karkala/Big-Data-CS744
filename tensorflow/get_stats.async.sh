#!/bin/bash


# Get the CPU utitlization
sar -u 10 200 >> cpu_stats_sync_output &
sar -r 10 200 >> mem_stats_sync_output &
sar -n  TCP 10 200 >> net_stats_sync_output &

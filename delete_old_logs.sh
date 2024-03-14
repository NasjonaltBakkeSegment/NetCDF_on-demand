#!/bin/bash

config_file="config/config.yml"
logs_keep_days=$(grep 'logs_keep_days' ${config_file} | awk '{print $2}')
tmp_logs_dir=$(grep 'tmp_logs_dir' ${config_file} | awk '{print $2}')

# Find all log files older than the defined age and delete them
find "$tmp_logs_dir" -type f -name "*.log" -mtime +$logs_keep_days -exec rm -f {} \;
#!/bin/bash

config_file="config/config.yml"
product_keep_hours=$(grep 'product_keep_hours' ${config_file} | awk '{print $2}')
lustre_NetCDFs_path=$(grep 'lustre_NetCDFs_path' ${config_file} | awk '{print $2}')

# Find all files with .nc suffix older than the defined age and delete them
find "$lustre_NetCDFs_path" -type f -name "*.nc" -mmin +$((product_keep_hours * 60)) -exec rm -f {} \;
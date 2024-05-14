#!/bin/bash
# Script to generate and serve the NetCDF on-demand pygeoapi server
source /modules/rhel8/conda/install/etc/profile.d/conda.sh
conda activate nbs_netcdf_ondemand

# This line is neccessary if changes have been made within the build subdirectory
python3 setup.py install

# If changes have been made to the configuration files, the server must be generated again using the code below
PYGEOAPI_CONFIG=pygeoapi-config.yml
PYGEOAPI_OPENAPI=openapi.yml
pygeoapi openapi generate $PYGEOAPI_CONFIG --output-file $PYGEOAPI_OPENAPI

# Serving the OpenAPI
pygeoapi serve

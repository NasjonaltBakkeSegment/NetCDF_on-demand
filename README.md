# NetCDF on-demand

## Setup
1. `git clone --recursive git@github.com:NasjonaltBakkeSegment/NetCDF_on-demand.git`
2. `conda create --name netcdf_ondemand`
3. `conda activate netcdf_ondemand`
4. `conda install -c conda-forge gdal`
5. `pip install shapely`
6. `pip install -r requirements.txt`

## Executing the script

Execute `main.py` with a comman-separated string of product names as an argument, for example
`python3 main.py --product_names='S1A_EW_GRDH_1SDH_20240227T070021_20240227T070128_052741_0661BB_2E43,S1A_EW_GRDH_1SDH_20240227T051739_20240227T051925_052740_0661B2_9C88'`

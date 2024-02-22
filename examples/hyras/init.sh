#!/bin/bash

# download the hyras Precipitation for the given years
mkdir -p ${DATA_FILE_PATH}/Precipitation
cd ${DATA_FILE_PATH}/Precipitation
for (( year=$START_YEAR; year<=$END_YEAR; year++ ))
do
    wget --no-clobber https://opendata.dwd.de/climate_environment/CDC/grids_germany/daily/hyras_de/precipitation/pr_hyras_1_${year}_v5-0_de.nc    
done

# download the hyras global radiation for the given years
mkdir -p ${DATA_FILE_PATH}/RadiationGlobal
cd ${DATA_FILE_PATH}/RadiationGlobal
for (( year=$START_YEAR; year<=$END_YEAR; year++ ))
do
    wget --no-clobber https://opendata.dwd.de/climate_environment/CDC/grids_germany/daily/hyras_de/radiation_global/rsds_hyras_5_${year}_v3-0_de.nc    
done

# download the hyras Pair humidity for the given years
mkdir -p ${DATA_FILE_PATH}/Humidity
cd ${DATA_FILE_PATH}/Humidity
for (( year=$START_YEAR; year<=$END_YEAR; year++ ))
do
    wget --no-clobber https://opendata.dwd.de/climate_environment/CDC/grids_germany/daily/hyras_de/humidity/hurs_hyras_5_${year}_v5-0_de.nc    
done

# download the hyras mean temperature for the given years
mkdir -p ${DATA_FILE_PATH}/TemperatureMean
cd ${DATA_FILE_PATH}/TemperatureMean
for (( year=$START_YEAR; year<=$END_YEAR; year++ ))
do
    wget --no-clobber https://opendata.dwd.de/climate_environment/CDC/grids_germany/daily/hyras_de/air_temperature_mean/tas_hyras_5_${year}_v5-0_de.nc    
done

# download the hyras min temperature for the given years
mkdir -p ${DATA_FILE_PATH}/TemperatureMin
cd ${DATA_FILE_PATH}/TemperatureMin
for (( year=$START_YEAR; year<=$END_YEAR; year++ ))
do
    wget --no-clobber https://opendata.dwd.de/climate_environment/CDC/grids_germany/daily/hyras_de/air_temperature_min/tasmin_hyras_5_${year}_v5-0_de.nc    
done

# download the hyras max temperature for the given years
mkdir -p ${DATA_FILE_PATH}/TemperatureMax
cd ${DATA_FILE_PATH}/TemperatureMax
for (( year=$START_YEAR; year<=$END_YEAR; year++ ))
do
    wget --no-clobber https://opendata.dwd.de/climate_environment/CDC/grids_germany/daily/hyras_de/air_temperature_max/tasmax_hyras_5_${year}_v5-0_de.nc    
done

# download the scripts directory
cd /tool_init/init
git clone https://github.com/vforwater/scripts.git
cd scripts
git pull

# install papermill and jupyter to run the examples
pip install papermill jupyter

# run the hyras example to import the downloaded data to the metacatalog instance
papermill ./hyras/upload_hyras.ipynb /tool_init/init/$(date +%F)_upload_hyras.ipynb -p DATA_DIR "${DATA_FILE_PATH}/{var}/*.nc" -p CONNECTION $METACATALOG_URI

# run the python script to build the example folder
cd /tool_init/init
python ./create_sample_runs.py
#!/bin/bash

# this is the first thing that is run when the installer service is started
# here we can use papermill to run the notebook for upload

# download the Copernicus GLO-30 dem for the given extent
mkdir -p ${DATA_FILE_PATH}/DEM
cd ${DATA_FILE_PATH}/DEM
for (( lat=$LAT_MIN; lat<=$LAT_MAX; lat++ ))
do
	for (( lon=$LON_MIN; lon<=$LON_MAX; lon++ ))
	do
		# Download the DEM data
		lat_leading_zeros=$(printf "%02d" $lat)
		lon_leading_zeros=$(printf "%03d" $lon)
		name=Copernicus_DSM_10_N${lat_leading_zeros}_00_E${lon_leading_zeros}_00
       	wget --no-clobber https://prism-dem-open.copernicus.eu/pd-desk-open-access/prismDownload/COP-DEM_GLO-30-DGED__2023_1/${name}.tar
		
		# Extract the DEM data
		tar -xf ${name}.tar

		# Move the .tif file to the current directory
		mv ${name}/DEM/${name}_DEM.tif .

		# Remove the files
		rm ${name}.tar
		rm -r ${name}
	done
done

# install papermill and jupyter to run the examples
pip install papermill jupyter

# run the dem example notebook
papermill /tool_init/init/upload_dem.ipynb /tool_init/init/$(date +%F)_upload_dem.ipynb -p DATA_DIR "${DATA_FILE_PATH}/DEM/*.tif" -p CONNECTION $METACATALOG_URI
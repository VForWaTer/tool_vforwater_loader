#!/bin/bash

# this is the first thing that is run when the installer service is started
# here we can use papermill to run the notebook for upload

# install papermill and jupyter to run the examples
pip install papermill jupyter

# run the dem example notebook
papermill /tool_init/init/upload_dem.ipynb /tool_init/init/$(date +%F)_upload_dem.ipynb -p DATA_DIR "${DATA_FILE_PATH}/DEM/*.tif" -p CONNECTION $METACATALOG_URI
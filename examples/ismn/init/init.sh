#!/bin/bash

# download the scripts directory
cd /tool_init/init
git clone https://github.com/vforwater/scripts.git
cd scripts
git pull

# install papermill and jupyter to run the examples
pip install papermill jupyter

# run the ISMN example to import the downloaded data to the metacatalog instance
papermill ./ismn/upload_ismn.ipynb /tool_init/init/$(date +%F)_upload_ismn.ipynb -p DATA_DIR $DATA_DIR -p CONNECTION $METACATALOG_URI

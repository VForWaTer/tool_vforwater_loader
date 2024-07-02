#!/bin/bash

# this is the first thing that is run when the installer service is started
# here we can use papermill to run the notebook for upload

# install papermill and jupyter to run the examples
pip install papermill jupyter

echo "This is the DEM init tool"
import os
from datetime import datetime as dt
from pathlib import Path
import shutil
import json

from json2args import get_parameter
from dotenv import load_dotenv
from metacatalog import api
from tqdm import tqdm

import pandas as pd
import xarray as xr

# parse parameters
kwargs = get_parameter()

# check if a toolname was set in env
toolname = os.environ.get('TOOL_RUN', 'vforwater_loader').lower()

# raise an error if the toolname is not valid
if toolname != 'vforwater_loader':
    raise AttributeError(f"[{dt.now().isocalendar()}] Either no TOOL_RUN environment variable available, or '{toolname}' is not valid.\n")


# extract the dataset ids
dataset_ids = kwargs['dataset_ids']

# extract the optional information
start = kwargs.get('start_date')
end = kwargs.get('end_date')
reference_area = kwargs.get('reference_area')

# check if a connection was given and if it is a valid path
if 'connection' in kwargs:
    if Path(kwargs['connection']).exists():
        load_dotenv(dotenv_path=kwargs['connection'])
    else:
        # it is interpreted as a connection uri
        connection = kwargs['connection']
else:
    load_dotenv()

# check if a connection evironment variable is given
if 'VFW_POSTGRES_URI' in os.environ:
    connection = os.environ['VFW_POSTGRES_URI']
elif 'METACATALOG_URI' in os.environ:
    connection = os.environ['METACATALOG_URI']
else:
    connection = None

# if we could not derive a connection, we hope for the best and hope that
# defaults are used
session = api.connect_database(connection)

# container for errors as thing could go wrong from here
errors = []
logs = []

# load the datasets
for dataset_id in tqdm(dataset_ids):
    try:
        results = api.find_entry(session, id=dataset_id, as_result=False)
        if len(results) > 1:
            raise Exception(f"Found more than one dataset with id {dataset_id}. (len = {len(results)})")
        elif len(results) == 0:
            raise Exception(f"Found no dataset with id {dataset_id}.")
        
        # load the dataset
        entry = results[0]

        # load the data
        data = entry.get_data(start=start, end=end)
        name = f"/out/{entry.variable.name.replace(' ', '_')}_{entry.id}"

        # pandas
        if isinstance(data, pd.DataFrame):
            # save to output folder
            data.to_parquet(f"{name}.parquet")
            logs.append(f"Saved dataset ID={entry.id} to {name}.parquet")
        
        # xarray
        elif isinstance(data, xr.Dataset):
            data.to_netcdf(f"{name}.nc")
            logs.append(f"Saved dataset ID={entry.id} to {name}.nc")
        
        # path
        elif isinstance(data, str):
            if Path(data).exists():
                new_loc = Path('/out') / Path(data).name
                shutil.copy(data, str(new_loc))
                logs.append(f"Saved dataset ID={entry.id} to {str(new_loc)}")
        
        # finally save the metadata - only JSON for now
        with open(f"{name}.json", 'w') as f:
            json.dump(entry.to_dict(deep=True, stringify=True), f, indent=4)

            
    except Exception as e:
        errors.append(f"ERRORED STEP LOAD DATASET: {str(e)}")
        continue



# ----------------------------
# output report

log_messages = "\n".join([f"  - {log}" for log in logs])

# build the message for now
MSG = f"""
This is the V-FOR-WaTer data loader report

The following information has been submitted to the tool:

START DATE:         {start}
END DATE:           {end}
REFERENCE AREA:     {reference_area is not None}

DATASET IDS:
{', '.join(map(str, dataset_ids))}

DATABASE CONNECTION: {connection is not None}
DATABASE URI:        {session.bind}

PROCESSING LOGS
---------------
{log_messages}

NOTE
----
The current version of the tool does not yet support clipping by reference area.
We are on it.
"""

# Add error messages
if len(errors) > 0:
    MSG += "\n\nERRORS:\n-------\nUnfortunately, there were errors during the processing of this tool. Please read them carefully:\n\n"
    MSG += "\n\n".join(errors)

    with open('/out/errors.txt', 'w') as f:
        f.write("\n".join(errors))

# print out the report
print(MSG)

# save it
with open('/out/process.txt', 'w') as f:
    f.write(MSG)

# save the logs on their own
with open('/out/logs.txt', 'w') as f:
    f.write("\n".join(logs))

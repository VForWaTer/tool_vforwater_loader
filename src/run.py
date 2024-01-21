import os
from datetime import datetime as dt
from pathlib import Path
import traceback
import warnings

from json2args import get_parameter
from dotenv import load_dotenv
from metacatalog import api
from tqdm import tqdm

from loader import load_entry

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
        entry = api.find_entry(session, id=dataset_id, return_iterator=True).one()
        
        # load the entry and return the data path
        #with warnings.catch_warnings(record=True) as warns:
            #warnings.simplefilter("ignore")
        out_path = load_entry(entry, start=start, end=end, reference_area=reference_area)
            #print(warns)

        # TODO: replace by logger
        logs.append(f"LOADED DATASET {dataset_id} TO {out_path}")

    except Exception as e:
        errors.append(f"ERRORED STEP LOAD DATASET: {str(e)}")

        # TODO: replace by logger
        with open(f'/out/errors_id_{dataset_id}.txt', 'w') as f:
            traceback.print_exc(file=f)
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

# print out the report
print(MSG)

# save it
with open('/out/process.txt', 'w') as f:
    f.write(MSG)

# save the logs on their own
with open('/out/logs.txt', 'w') as f:
    f.write("\n".join(logs))

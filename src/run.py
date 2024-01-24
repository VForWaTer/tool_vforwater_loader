import os
from datetime import datetime as dt
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

from json2args import get_parameter
from dotenv import load_dotenv
from metacatalog import api
from tqdm import tqdm

from loader import load_entry_data
from logger import logger
from clip import reference_area_to_file

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

# initialize a new log-file by overwriting any existing one
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

NOTE
----
The current version of the tool does only support netCDF clips using a bounding box.
The current version of the tool does only select files within a time range for multi-file netCDFs.

Processing logs:
----------------
"""
with open('/out/processing.log', 'w') as f:
    f.write(MSG)

# --------------------------------------------------------------------------- #
# Here is the actual tool
# save the reference area to a file for later reuse
if reference_area is not None:
    reference_area = reference_area_to_file(reference_area)

# load the datasets
with ProcessPoolExecutor() as executor:
    for dataset_id in tqdm(dataset_ids):
        try:
            entry = api.find_entry(session, id=dataset_id, return_iterator=True).one()
            
            # load the entry and return the data path
            data_path = load_entry_data(entry, executor, start=start, end=end, reference_area=reference_area)
            

        except Exception as e:
            logger.exception(f"ERRORED on dataset <ID={dataset_id}>")
            continue
    
    # wait until all results are finished
    executor.shutdown(wait=True)
    logger.info(f"Pool {type(executor).__name__} finished all tasks and shutdown.")

    # here to the stuff for creating a consistent dataset

# --------------------------------------------------------------------------- #

# print out the report
with open('/out/processing.log', 'r') as f:
    print(f.read())


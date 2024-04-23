import os
import sys
from datetime import datetime as dt
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor as PoolExecutor

from json2args import get_parameter
from dotenv import load_dotenv
from metacatalog import api
from tqdm import tqdm

from param import load_params, Integrations
from loader import load_entry_data
from logger import logger
import ingestor
import aggregator
import reporter
from clip import reference_area_to_file

# parse parameters
kwargs = get_parameter()

# check if a toolname was set in env
toolname = os.environ.get('TOOL_RUN', 'vforwater_loader').lower()

# raise an error if the toolname is not valid
if toolname != 'vforwater_loader':
    raise AttributeError(f"[{dt.now().isocalendar()}] Either no TOOL_RUN environment variable available, or '{toolname}' is not valid.\n")


# use the pydantic model to handle the input parameters
params = load_params(**kwargs)

# check if a connection was given and if it is a valid path
# this is handled extra to keep the pydantic model simpler
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
MSG = f"""\
This is the V-FOR-WaTer data loader report

The following information has been submitted to the tool:

START DATE:         {params.start_date}
END DATE:           {params.end_date}
REFERENCE AREA:     {params.reference_area is not None}
INTEGRATION:        {params.integration}
KEEP DATA FILES:    {params.keep_data_files}

DATASET IDS:
{', '.join(map(str, params.dataset_ids))}

DATABASE CONNECTION: {connection is not None}
DATABASE URI:        {session.bind}

AGGREGATION SETTINGS
--------------------
PRECISION:          {params.precision}
RESOLUTION:         {params.resolution}x{params.resolution}
TARGET CRS:         EPSG:3857

Processing logs:
----------------
"""
with open('/out/processing.log', 'w') as f:
    f.write(MSG)

# if the integration is set to NONE and the user does not want to keep the data files, there will be no output
if params.integration == Integrations.NONE and not params.keep_data_files:
    logger.critical("You have set the integration to NONE and do not want to keep the data files. This will result in no output.")
    sys.exit(1)

# --------------------------------------------------------------------------- #
# Here is the actual tool
tool_start = time.time()

# debug the params before we do anything with them
logger.debug(f"JSON dump of parameters received: {params.model_dump_json()}")

# save the reference area to a file for later reuse
if params.reference_area is not None:
    reference_area = reference_area_to_file()

# load the datasets
# save the entries and their data_paths for later use
file_mapping = []
with PoolExecutor() as executor:
    logger.debug(f"START {type(executor).__name__} - Pool to load and clip data source files.")
    for dataset_id in tqdm(params.dataset_ids):
        try:
            entry = api.find_entry(session, id=dataset_id, return_iterator=True).one()
            
            # load the entry and return the data path
            data_path = load_entry_data(entry, executor)

            # save the mapping from entry to data_path
            file_mapping.append({'entry': entry, 'data_path': data_path})
        except Exception as e:
            logger.exception(f"ERRORED on dataset <ID={dataset_id}>")
            continue
    
    # wait until all results are finished
    executor.shutdown(wait=True)
    logger.debug(f"STOP {type(executor).__name__} - Pool finished all tasks and shutdown.")

# here to the stuff for creating a consistent dataset
# check if the user disabled integration
if params.integration == Integrations.NONE:
    logger.debug("Integration is disabled. No further processing will be done.")

# check if we have any files to process
elif len(file_mapping) > 0:
    logger.info(f"Starting to create a consistent DuckDB dataset at {params.database_path}. Check out https://duckdb.org/docs/api/overview to learn more about DuckDB.")
    
    # start a timer 
    t1 = time.time()
    path = ingestor.load_files(file_mapping=file_mapping)
    t2 = time.time()
    logger.info(f"Finished creating the dataset at {path} in {t2-t1:.2f} seconds.")
else:
    logger.warning("It seems like no data files have been processed. This might be an error.")

# switch the type of integrations
if params.integration != Integrations.NONE:
    with PoolExecutor() as executor:
        logger.debug(f"START {type(executor).__name__} - Pool to ingest data files into a Dataset DuckDB database.")

        if params.integration == Integrations.TEMPORAL or params.integration == Integrations.ALL:
            # run the temporal aggregation
            aggregator.aggregate_scale(aggregation_scale='temporal', executor=executor)
        
        if params.integration == Integrations.SPATIAL or params.integration == Integrations.ALL:
            # run the spatial aggregation
            aggregator.aggregate_scale(aggregation_scale='spatial', executor=executor)
        
        if params.integration == Integrations.SPATIO_TEMPORAL or params.integration == Integrations.ALL:
            # run the spatio-temporal aggregation
            aggregator.aggregate_scale(aggregation_scale='spatiotemporal', executor=executor)

        # wait until all results are finished
        executor.shutdown(wait=True)
        logger.debug(f"STOP {type(executor).__name__} - Pool finished all tasks and shutdown.")


    # finally run a thrid pool to generate reports
    with PoolExecutor() as executor:
        logger.debug(f"START {type(executor).__name__} - Pool to generate final reports.")

    # create a callback to log exceptions
    def callback(future):
        exc = future.exception() 
        if exc is not None:
            logger.exception(exc)

        # generate the profile report - start first as this one might potentially take longer
        # TODO: there should be an option to disable this
        executor.submit(reporter.generate_profile_report).add_done_callback(callback)

        # generate the readme
        executor.submit(reporter.generate_readme).add_done_callback(callback)


        # wait until all results are finished
        executor.shutdown(wait=True)
        logger.debug(f"STOP {type(executor).__name__} - Pool finished all tasks and shutdown.")
# --------------------------------------------------------------------------- #


# we're finished.
t2 = time.time()
logger.info(f"Total runtime: {t2 - tool_start:.2f} seconds.")

# print out the report
with open('/out/processing.log', 'r') as f:
    print(f.read())


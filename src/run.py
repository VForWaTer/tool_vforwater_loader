import os
import sys
import platform
import time
from concurrent.futures import ThreadPoolExecutor as PoolExecutor

from json2args import get_parameter
from json2args.logger import logger
from dotenv import load_dotenv

from tqdm import tqdm
from metacatalog_api import core
from metacatalog_api import __version__ as metacatalog_version
from param import Params
from loader import load_entry_data
from utils import reference_area_to_file
from version import __version__

# always load .env files
load_dotenv()

# parse parameters
kwargs = get_parameter(typed=False)
params = Params(**kwargs)

# check if a toolname was set in env
toolname = os.environ.get('TOOL_RUN', 'vforwater_loader').lower()

# raise an error if the toolname is not valid
if toolname != 'vforwater_loader':
    raise AttributeError("Either no TOOL_RUN environment variable available, or '{toolname}' is not valid.\n")

# test the database connection
try:
    with core.connect() as session:
        URI = session.bind.url
        MC_VERSION = core.db.get_db_version(session, 'public')['db_version']
except Exception as e:
    logger.error(f"Could not connect to the database: {e}")
    sys.exit(1)

# initialize a new log-file by overwriting any existing one
# build the message for now
MSG = f"""\
This is the V-FOR-WaTer data loader report

The loader version is: {__version__} (Python: {platform.python_version()})
Running on: {platform.platform()}

The following information has been submitted to the tool:

START DATE:         {params.start_date}
END DATE:           {params.end_date}
REFERENCE AREA:     {params.reference_area is not None}
CELL TOUCHES:       {params.cell_touches}

DATASET IDS:
{', '.join(map(str, params.dataset_ids))}

The MetaCatalog API version is:  {metacatalog_version}
DATABASE URI:                    {URI}
DB Schema Version remote:        {MC_VERSION}
DB Schema Version required:      {core.db.DB_VERSION}
Version mismatch:                {MC_VERSION < core.db.DB_VERSION}

Processing logs:
----------------
"""
with open('/out/processing.log', 'w') as f:
    f.write(MSG)

# handle the version mismatch
if MC_VERSION < core.db.DB_VERSION:
    if os.environ.get('MC_FORCE_MIGRATION', 'false').lower() == 'true':
        logger.info("MC_FORCE_MIGRATION is set to true. Proceeding with migration.")
        core.migrate_db()
    else:
        logger.error(f"DB Schema version mismatch. The Loader requires version {core.db.DB_VERSION} but the remote DB has version {MC_VERSION}. You can use metacatalog_api.core.migrate_db() to run the migration to version {core.db.DB_VERSION}.")
        sys.exit(1)
    

# Here is the actual tool
# --------------------------------------------------------------------------- #
# mark the start of the tool
logger.info(f"#TOOL START - Vforwater Loader - {__version__}")
tool_start = time.time()

# debug the params before we do anything with them
#logger.debug(f"JSON dump of parameters received: {params.model_dump_json()}")

# save the reference area to a file for later reuse
if params.reference_area is not None:
    reference_area = reference_area_to_file(params)

# load the datasets
# save the entries and their data_paths for later use
file_mapping = []
with PoolExecutor() as executor:
    logger.debug(f"START {type(executor).__name__} - Pool to load and clip data source files.")
    logger.info(f"A total of {len(params.dataset_ids)} are requested. Start loading data sources.")
    
    for dataset_id in tqdm(params.dataset_ids):
        try:
            matches = core.entries(ids=dataset_id)
            if len(matches) == 0:
                logger.error(f"Could not find dataset <ID={dataset_id}>.")
                continue
            elif len(matches) > 1:
                logger.warning(f"Found multiple datasets with ID <ID={dataset_id}>. Using the first one.")
                entry = matches[0]
            else:
                entry = matches[0]
            
            # load the entry and return the data path
            data_path = load_entry_data(entry, executor, params)

            # if data_path is None, we skip this step
            if data_path is None:
                logger.error(f"Could not load data for dataset <ID={dataset_id}>. The content of '/out/datasets' might miss something.")
                continue

            # save the mapping from entry to data_path
            file_mapping.append({'entry': entry, 'data_path': data_path})
        except Exception as e:
            logger.exception(f"ERRORED on dataset <ID={dataset_id}>.\nError: {str(e)}")
            continue
    
    # wait until all results are finished
    executor.shutdown(wait=True)
    logger.info(f"STOP {type(executor).__name__} - Pool finished all tasks and shutdown.")

# we're finished.
t2 = time.time()
logger.info(f"Total runtime: {t2 - tool_start:.2f} seconds.")
logger.info("#TOOL END")

# print out the report
with open('/out/processing.log', 'r') as f:
    print(f.read())


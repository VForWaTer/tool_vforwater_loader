from typing import Union
import time
from concurrent.futures import Executor, Future
from pathlib import Path
from datetime import datetime as dt
import json
from decimal import Decimal
import shutil

from metacatalog.models import Entry
from dask.dataframe import DataFrame as DaskDataFrame
import pandas as pd
import xarray as xr

from logger import logger


# create a custom serializer for Entry dict
class EntryDictSerializer(json.JSONEncoder):
    def default(self, obj):
        # handle Decial and datetime differently
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, dt):
            return obj.isoformat()

        # use the default method
        return super().default(obj)


def dispatch_save_file(entry: Entry, data: Union[pd.DataFrame, DaskDataFrame, xr.Dataset], executor: Executor, base_path: str = '/out') -> Future:
    # get the target_name
    target_path = Path(base_path) / f"{entry.variable.name.replace(' ', '_')}_{entry.id}"

    # define the exception handler
    def exception_handler(future: Future):
        exc = future.exception()
        if exc is not None:
            logger.error(f"ERRORED on saving dataset <ID={entry.id}> to {target_path}")
        else:
            # save the metadata
            metafile_name = f"{target_path}.json"
            entry_metadata_saver(entry, metafile_name)
            logger.info(f"Saved metadata for dataset <ID={entry.id}> to {metafile_name}.")
            
    # switch the data type
    if isinstance(data, (pd.DataFrame, DaskDataFrame)):
        target_name = f"{target_path}.parquet"
        future = executor.submit(dataframe_to_parquet_saver, data, target_name)
        future.add_done_callback(exception_handler)
    else:
        future = executor.submit(raw_data_copy_saver, entry, target_path)


def dataframe_to_parquet_saver(data: Union[pd.DataFrame, DaskDataFrame], target_name: str) -> str:
    t1 = time.time()
    if isinstance(data, pd.DataFrame):
        data.to_parquet(target_name, index=False)
    elif isinstance(data, DaskDataFrame):
        data.to_parquet(target_name, write_index=False)
    else:
        logger.error(f"Could not save {target_name} as it is not a pandas or dask dataframe. Got a {type(data)} instead.")
    t2 = time.time()

    # after finishing add a log message
    logger.info(f"Finished writing {target_name} after {t2-t1:.2f} seconds.")

    return target_name


def entry_metadata_saver(entry: Entry, target_name: str) -> str:
    # get the dictionary
    entry_dict = entry.to_dict(deep=True, stringify=False)

    # create the json with the custom serializer
    with open(target_name, 'w') as f:
        json.dump(entry_dict, f, cls=EntryDictSerializer, indent=4)
    
    return target_name


def raw_data_copy_saver(entry: Entry, target_name: Union[str, Path]) -> str:
    # warn the user
    logger.warning(f"Datasource <ID={entry.id}> is falling back to raw data-copy, as this tool does not include a specified writer. Let's hope the best.")
    
    # get the path
    source_path = Path(entry.datasource.path)

    # check if that exists
    if not source_path.exists():
        logger.error(f"Could not find the raw data source {source_path}.")
        return None
    
    # simply copy the data over
    target_name = Path(target_name) / 'raw' / source_path.name
    if not target_name.exists():
        target_name.mkdir(parents=True, exist_ok=True)
        if '*' in str(source_path):            
            shutil.copytree(str(source_path), str(target_name))
        else:
            shutil.copy(str(source_path), str(target_name))
    
    logger.info(f"Finished copying raw data from {source_path} to {target_name}.")
    return str(source_path)

        
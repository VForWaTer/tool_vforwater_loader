import json
import shutil
import time
from concurrent.futures import Executor, Future
from datetime import datetime as dt
from decimal import Decimal
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import polars as pl
import xarray as xr
from dask.dataframe import DataFrame as DaskDataFrame
from json2args.logger import logger
from metacatalog_api.models import Metadata

# create a union of all supported Dataframe types
DataFrame = Union[pd.DataFrame, DaskDataFrame, pl.DataFrame]


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


# TODO: target path should be createable from the outside
def dispatch_save_file(
    entry: Metadata, data: DataFrame | xr.Dataset, executor: Executor, base_path: str = "/out", target_name: str | None = None, save_meta: bool = True
) -> Future:
    # get the target_name
    if target_name is None:
        target_path = Path(base_path) / f"{entry.variable.name.replace(' ', '_')}_{entry.id}"
    else:
        target_path = Path(base_path) / target_name

    # log out the target path
    logger.debug(f"target_path derived for <ID={entry.id}> is: {target_path}")

    # define the exception handler
    # TODO exception handler is not the right name anymore
    def exception_handler(future: Future):
        exc = future.exception()
        if exc is not None:
            logger.error(f"ERRORED on saving dataset <ID={entry.id}> to {target_path}")
        elif save_meta:
            # save the metadata
            metafile_name = f"{target_path}.metadata.json"
            entry_metadata_saver(entry, metafile_name)
            logger.info(f"Saved metadata for dataset <ID={entry.id}> to {metafile_name}.")

    # switch the data type
    if isinstance(data, (pd.DataFrame, DaskDataFrame, pl.DataFrame)):
        if str(target_path).endswith("csv"):
            future = executor.submit(dataframe_to_csv_saver, data, target_path)
        else:
            if not str(target_path).endswith(".parquet"):
                target_path = f"{target_path}.parquet"
            future = executor.submit(dataframe_to_parquet_saver, data, target_path)
    elif isinstance(data, xr.Dataset):
        if not str(target_path).endswith(".nc"):
            target_path = f"{target_path}.nc"
        future = executor.submit(xarray_to_netcdf_saver, data, target_path)
    else:
        future = executor.submit(raw_data_copy_saver, entry, target_path)

    # add the exception handler
    future.add_done_callback(exception_handler)
    return future


def dispatch_result_saver(file_name: str, data: DataFrame, executor: Executor) -> Future:
    # define an exception handler
    def exception_handler(future: Future):
        exc = future.exception()
        if exc is not None:
            logger.error(f"Saving result file {file_name} errored: {str(exc)}")

    # switch the data type:
    if isinstance(data, (pd.DataFrame, DaskDataFrame, pl.DataFrame)):
        future = executor.submit(dataframe_to_parquet_saver, data, file_name)
    else:
        raise NotImplementedError(f"Right now, the result handler can only dispatch save actions for DataFrames. Got a {type(data)} instead.")

    # add the exception handler
    future.add_done_callback(exception_handler)
    return future


def dataframe_to_parquet_saver(data: DataFrame, target_name: str) -> str:
    t1 = time.time()
    if isinstance(data, pd.DataFrame):
        data.to_parquet(target_name, index=False)
    elif isinstance(data, DaskDataFrame):
        for partition in data.partitions:
            partition.compute().to_parquet(target_name, append=True, index=False)
    elif isinstance(data, pl.DataFrame):
        data.write_parquet(target_name)
    else:
        logger.error(f"Could not save {target_name} as it is not a pandas or dask dataframe. Got a {type(data)} instead.")
    t2 = time.time()

    # after finishing add a log message
    logger.info(f"Finished writing {target_name} after {t2 - t1:.2f} seconds.")

    return target_name


def dataframe_to_csv_saver(data: DataFrame, target_name: str) -> str:
    t1 = time.time()
    if isinstance(data, pd.DataFrame):
        data.to_csv(target_name, index=True)
    elif isinstance(data, DaskDataFrame):
        for partition in data.partitions:
            partition.compute().to_csv(target_name, index=True)
    elif isinstance(data, pl.DataFrame):
        data.write_csv(target_name)
    else:
        logger.error(f"Could not save {target_name} as it is not a pandas, polars or dask dataframe. Got a {type(data)} instead.")
    t2 = time.time()

    logger.info(f"Finished writing {target_name} after {t2 - t1:.2f} seconds.")
    return target_name


def xarray_to_netcdf_saver(data: xr.Dataset, target_name: str) -> str:
    # the netCDF is may already be written by the extracting process if CDO was used
    if Path(target_name).exists():
        logger.debug(f"writer.xarray_to_netcdf_saver: {target_name} already exists. Skipping.")
        return target_name

    t1 = time.time()
    data.to_netcdf(target_name)
    t2 = time.time()

    # after finishing add a log message
    logger.info(f"Finished writing {target_name} after {t2 - t1:.2f} seconds.")

    return target_name


def entry_metadata_saver(entry: Metadata, target_name: str) -> str:
    entry_json = entry.model_dump_json(indent=4)
    with open(target_name, "w") as f:
        f.write(entry_json)

    return target_name


def raw_data_copy_saver(entry: Metadata, target_name: str | Path) -> str:
    # warn the user
    logger.warning(f"Datasource <ID={entry.id}> is falling back to raw data-copy, as this tool does not include a specified writer. Let's hope the best.")

    # get the path
    source_path = Path(entry.datasource.path)

    # simply copy the data over
    target_name = Path(target_name) / "raw" / source_path.name
    try:
        target_name.parent.mkdir(parents=True, exist_ok=True)
        if "*" in str(source_path):
            shutil.copytree(str(source_path), str(target_name))
        else:
            shutil.copy(str(source_path), str(target_name))
    except FileNotFoundError:
        logger.error(f"Could not copy raw data from {source_path} [NOT FOUND].")
        return None

    logger.info(f"Finished copying raw data from {source_path} to {target_name}.")
    return str(source_path)

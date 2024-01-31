from typing import List, TypedDict, Optional
import time as time
import glob
from pathlib import Path

from tqdm import tqdm
import rioxarray
import xarray as xr
import geopandas as gpd
from dask.dataframe import DataFrame
from geocube.api.core import make_geocube
import duckdb
from metacatalog.models import Entry

from logger import logger
from param import load_params


class FileMapping(TypedDict):
    entry: Entry
    data_path: str


def _table_exists(table_name: str) -> bool:
    # load the parameters
    params = load_params()

    # check if the database exists at all
    database_path = Path(params.database_path)
    if not database_path.exists():
        return False
    
    # check for the table
    with duckdb.connect(database=str(database_path), read_only=True) as db:
        count = len(db.sql(f"SELECT 'exists' FROM information_schema.tables WHERE table_name='{table_name}';").fetchall())
    if count > 0:
        return True
    else:
        return False


def _create_datasource_table(entry: Entry, table_name: str, use_spatial: bool = False) -> str:
    if use_spatial:
        raise NotImplementedError('There is still an error with the spatial type.')
    # get the parameters
    params = load_params()

    # get the dimension names
    spatial_dims = entry.datasource.spatial_scale.dimension_names
    temporal_dims = entry.datasource.temporal_scale.dimension_names
    variable_dims = entry.datasource.variable_names
    
    # container for the coulumn names
    column_names = []
    
    # tempral dimensions
    for dim in temporal_dims:
       column_names.append(f" {dim} TIMESTAMP")
    
    # spatial dimensions
    if len(spatial_dims) == 2 and use_spatial:
        column_names.append(f" cell BOX_2D")
    else:
        column_names.append(' ' + ','.join([f" {dim} DOUBLE" for dim in spatial_dims]))
    
    # variable dimensions
    column_names.append(' ' + ','.join([f" {dim} DOUBLE" for dim in variable_dims]))

    # build the sql statement
    sql = f"CREATE TABLE {table_name} ({','.join(column_names)});"

    # get the database path
    dbname = str(params.database_path)

    # run the sql statement
    with duckdb.connect(database=dbname, read_only=False) as db:
        db.install_extension('spatial')
        db.load_extension('spatial')
        logger.info(f"duckdb {dbname} -c \"{sql}\"")
        db.execute(sql)
    
    return dbname


def _create_insert_sql(entry: Entry, table_name: str, source_name: str = 'df', use_spatial: bool = False) -> str:
    if use_spatial:
        raise NotImplementedError('There is still an error with the spatial type.')
    
    # get the parameters
    params = load_params()

    # get the dimension names
    spatial_dims = entry.datasource.spatial_scale.dimension_names
    temporal_dims = entry.datasource.temporal_scale.dimension_names
    variable_dims = entry.datasource.variable_names

    # build the SQL
    sql = f"INSERT INTO {table_name} SELECT "

    # containe for selecting column names
    column_names = []

    # tempral dimensions
    if len(temporal_dims) > 0:
        column_names.append(f" {', '.join(temporal_dims)} ")

    # spatial dimensions
    if len(spatial_dims) == 2 and use_spatial:
       column_names.append(f" ({','.join(spatial_dims)})::BOX_2D AS cell ")
    else:
        column_names.append(f" {', '.join(spatial_dims)} ")

    # variable dimensions
    column_names.append(f" {', '.join(variable_dims)} ")

    # close the sql statement
    final_sql = f"{sql} {','.join(column_names)} FROM {source_name};"

    return final_sql


def load_files(file_mapping: List[FileMapping]) -> str:
    # get the parameters
    params = load_params()

    # go for the data-sources
    for mapping in tqdm(file_mapping):
        # unpack the mapping
        entry = mapping['entry']
        data_path = Path(mapping['data_path'])

        # data path might be a directory
        if data_path.is_dir():
            files = glob.glob(str(data_path / '**' / '*'), recursive=True)
        else:
            files = [str(data_path)]
        
        # load all files composing this data source into the database
        for fname in files:
            # handle import 
            try:
                _switch_source_loader(entry, fname)
            except:
                logger.exception(f"ERRORED on loading file <{fname}>")
                continue
        
    # now load all metadata that we can find on the dataset folder level
    load_metadata_to_duckdb()
    
    # return the database path
    return str(params.database_path)


def _switch_source_loader(entry: Entry, file_name: str) -> str:
    # get the suffix of this file
    suf = Path(file_name).suffix.lower()
    
    # switch for the different file types that are supported
    if suf in ('.nc', 'netcdf', 'cdf', '.nc4'):
        ds = xr.open_dataset(file_name, decode_coords='all', mask_and_scale=True, chunks='auto')
        return load_xarray_to_duckdb(entry, ds)
    elif suf == '.parquet':
        return load_parquet_to_duckdb(entry, file_name)
    elif suf == 'csv':
        raise NotImplementedError('CSV file import are currently not supported.')
    elif suf in ('.tif', '.tiff', '.geotiff'):
        raise NotImplementedError('GeoTIFF file import are currently not supported.')
    else:
        raise RuntimeError(f"Unknown file type <{suf}> for file <{file_name}>.")
    

def load_xarray_to_duckdb(entry: Entry, data: xr.Dataset) -> str:
    # get the parameters
    params = load_params()

    # get the dimension names and spatial dimensions
    dimension_names = entry.datasource.dimension_names

    # we assume that the source uses chunking, hence convert to dask dataframe
    logger.info(f"Loading preprocessed source <ID={entry.id}> to duckdb database <{params.database_path}> for data integration...")
    t1 = time.time()
    
    # get a delayed dask dataframe
    ddf = data.to_dask_dataframe()[dimension_names]

    # create the table name
    table_name = f"{entry.variable.name.replace(' ', '_')}_{entry.id}"
    # check if the table exists
    if not _table_exists(table_name):
        _create_datasource_table(entry=entry, table_name=table_name)

    # now go for each partition
    is_first = True
    for partition in ddf.partitions:
        # compute partition to make the 
        df = partition.compute()
        logger.info(f"python - dfs = [data.to_dask_dataframe()[{dimension_names}].partitions[i].compute() for i in range({ddf.npartitions})]")

        # load to duckdb
        with duckdb.connect(database=str(params.database_path), read_only=False) as db:
            db.load_extension('spatial')
            sql = _create_insert_sql(entry, table_name)

            # build the sql statement
            db.execute(sql)

            # log only one
            if is_first:
                logger.info(f"duckdb - FOREACH df in dfs - {sql}")
                is_first = False

    # log the time
    t2 = time.time()
    logger.info(f"took {t2-t1:.2f} seconds")

    return str(params.database_path)


def load_parquet_to_duckdb(entry: Entry, file_name: str) -> str:
    # get the parameters
    params = load_params()

    # logging
    logger.info(f"Loading preprocessed source <ID={entry.id}> to duckdb database <{params.database_path}> for data integration...")
    t1 = time.time()

    # derive the table name
    # TODO: there should only be one place to derive this name
    table_name = f"{entry.variable.name.replace(' ', '_')}_{entry.id}"

    # check if the table exists
    if not _table_exists(table_name):
        _create_datasource_table(entry=entry, table_name=table_name)

    # derive the sql statement
    sql = _create_insert_sql(entry, table_name, source_name=f"'{file_name}'")

    # load the data
    with duckdb.connect(database=str(params.database_path), read_only=False) as db:
        db.load_extension('spatial')
        db.execute(sql)
        logger.info(f"duckdb - {sql}")
    
    # finish
    t2 = time.time()
    logger.info(f"took {t2-t1:.2f} seconds")


def load_metadata_to_duckdb() -> str:
    # get the parameters
    params = load_params()

    # start a timer
    t1 = time.time()
    
    # build the sql statement to load all metadata
    meta_paths = str(params.dataset_path / '*.metadata.json')
    
    # check if the metadata table exists
    if not _table_exists('metadata'):
        logger.debug(f"Database {params.database_path} does not contain a table 'metadata'. Creating it now...")
        sql = f"CREATE TABLE metadata AS SELECT * FROM '{meta_paths}';"
    else:
        sql = f"INSERT INTO metadata SELECT * FROM '{meta_paths}';"

    # get a connection to the database and run the command
    with duckdb.connect(database=str(params.database_path), read_only=False) as db:
        # execute
        db.execute(sql)
        logger.info(f"duckdb - {sql}")

    # stop the timer
    t2 = time.time()
    logger.info(f"took {t2-t1:.2f} seconds")

    return str(params.database_path)


# integration views
def _get_database_path(database_path: Optional[str] = None) -> str:
    # check if we have a database path
    if database_path is None:
        params = load_params()
        database_path = str(params.database_path)
    
    # get the database name
    return database_path

def list_tables(database_path: Optional[str] = None) -> List[str]:
    # check if we have a database path
    db_path = _get_database_path(database_path)
    
    # create the sql
    sql = "SELECT table_name FROM information_schema.tables WHERE table_name not like '%_aggregate' AND table_name != 'metadata';"

    # connect to the database and run
    with duckdb.connect(database=db_path, read_only=True) as db:
        tables = db.sql().fetchnumpy().get('table_name').tolist()
    
    # return the tables
    return tables


def add_spatial_integration(entry: Entry, table_name: str, database_path: Optional[str] = None):
    # check if we have a database path
    db_path = _get_database_path(database_path)

    # 
from typing import List, TypedDict, Optional
import time as time
import glob
from pathlib import Path

from tqdm import tqdm
import rioxarray
import xarray as xr
import duckdb
from metacatalog.models import Entry

from logger import logger
from param import load_params

SPATIAL_DIMENSIONS = ('lon', 'lat', 'z')


AGGREGATIONS = dict(
    mean="AVG({variable}) AS mean",
    std="STDDEV({variable}) AS std",
    kurtosis="KURTOSIS({variable}) AS kurtosis",
    skewness="SKEWNESS({variable}) AS skewness",
    median="MEDIAN({variable}) AS median",
    min="MIN({variable}) AS min",
    max="MAX({variable}) AS max",
    sum="SUM({variable}) AS sum",
    count="COUNT({variable}) AS count",
    q25="quantile_disc({variable}, 0.25) as quartile_25",
    q75="quantile_disc({variable}, 0.75) as quartile_75",
    entropy="entropy({variable}) as entropy",
#    geomean="GEOMEAN({variable}) as geomean",              # does not exist for v0.8.1!
    histogram="histogram({variable}) as histogram"
)


CellAlignFunc = dict(
    floor = 'FLOOR',
    ceil='CEIL',
    round='ROUND',
    center='ROUND',
    centroid='ROUND',
    lowerleft='FLOOR',
    upperright='CEIL',
)


# class AggregationLevel(Enum):
#     second = 'second'
#     minute = 'minute'
#     hour = 'hour'
#     day = 'day'
#     month = 'month'
#     year = 'year'
#     decade = 'decade'
#     century = 'century'


class FileMapping(TypedDict):
    entry: Entry
    data_path: str


AGGREGATION_VIEW = """CREATE OR REPLACE VIEW aggregations AS WITH aggs AS 
(SELECT str_split(function_name, '_') AS parts, * FROM duckdb_functions() WHERE function_type='table_macro' AND schema_name='main' AND parts[-1] == 'aggregate')
SELECT parts[-3]::int as id, array_to_string(parts[:-3], '_') as variable, array_to_string(parts[:-2], '_') as data_table, parts[-2] as aggregation_scale, function_name, parameters, from aggs;
"""

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


def _create_datasource_table(entry: Entry, table_name: str) -> str:
    # get the parameters
    params = load_params()

    # get the dimension names
    spatial_dims = entry.datasource.spatial_scale.dimension_names if entry.datasource.spatial_scale is not None else []
    temporal_dims = entry.datasource.temporal_scale.dimension_names if entry.datasource.temporal_scale is not None else []
    variable_dims = entry.datasource.variable_names
    
    # container for the coulumn names
    column_names = []
    
    # tempral dimensions
    if len(temporal_dims) > 0:
       column_names.append(f" time TIMESTAMP")
    
    # spatial dimensions
    if len(spatial_dims) == 2 and params.use_spatial:
        column_names.append(f" cell BOX_2D")
    else:
        column_names.append(' ' + ','.join([f" {name} DOUBLE" for dim, name in zip(spatial_dims, SPATIAL_DIMENSIONS)]))
    
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


def _create_insert_sql(entry: Entry, table_name: str, source_name: str = 'df') -> str:
    params = load_params()

    # get the dimension names
    spatial_dims = entry.datasource.spatial_scale.dimension_names if entry.datasource.spatial_scale is not None else []
    temporal_dims = entry.datasource.temporal_scale.dimension_names if entry.datasource.temporal_scale is not None else []
    variable_dims = entry.datasource.variable_names

    # build the SQL
    sql = f"INSERT INTO {table_name} SELECT "

    # containe for selecting column names
    column_names = []

    # tempral dimensions
    if len(temporal_dims) > 0:
        column_names.append(f" {temporal_dims[0]} as time ")

    # spatial dimensions
    if len(spatial_dims) == 2 and params.use_spatial:
       column_names.append(f" ({','.join(spatial_dims)})::BOX_2D AS cell ")
    else:
        column_names.append(' ' + ', '.join([f"{dim} AS {name}" for dim, name in zip(spatial_dims, SPATIAL_DIMENSIONS)]))

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
        table_names = []
        for fname in files:
            # handle import 
            try:
                table_name = _switch_source_loader(entry, fname)
                table_names.append(table_name)
            except Exception as e:
                logger.exception(f"ERRORED on loading file <{fname}>")
                logger.error(str(e))
                continue
        
        # get a set of all involved table names
        table_names = set(table_names)
        
        # create the prepared aggregation statement for each table
        for table_name in table_names:
            # temporal aggregation
            try:
                add_temporal_integration(entry=entry, table_name=table_name, funcs=None)
            except Exception as e:
                logger.exception(f"ERRORED on adding temporal integration for table <{table_name}>")
                logger.error(str(e))
            
            # spatial aggregation
            try:
                # TODO: the hard coded params should be changeable
                add_spatial_integration(entry=entry, table_name=table_name, funcs=None, target_epsg=3857, algin_cell='center')
            except Exception as e:
                logger.exception(f"ERRORED on adding spatial integration for table <{table_name}>")
                logger.error(str(e))
            
            # spatio-temporal aggregation
            try:
                # TODO: the hard coded params should be changeable
                add_spatial_integration(entry=entry, table_name=table_name, spatio_temporal=True, funcs=None, target_epsg=3857, algin_cell='center')
            except Exception as e:
                logger.exception(f"ERRORED on adding spatio-temporal integration for table <{table_name}>")
                logger.error(str(e))
        
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
        ds = xr.open_dataset(file_name, mask_and_scale=True, chunks='auto')
        return load_xarray_to_duckdb(entry, ds)
    else:
        raise RuntimeError(f"Unknown file type <{suf}> for file <{file_name}>.")
    

def load_xarray_to_duckdb(entry: Entry, data: xr.Dataset) -> str:
    # get the parameters
    params = load_params()

    # get the dimension names and spatial dimensions
    dimension_names = entry.datasource.dimension_names

    # log out the dimension names
    logger.debug(f"Dimension names for <ID={entry.id}>: {dimension_names}")

    # we assume that the source uses chunking, hence convert to dask dataframe
    logger.info(f"Loading preprocessed source <ID={entry.id}> to duckdb database <{params.database_path}> for data integration...")
    t1 = time.time()
    
    # get a delayed dask dataframe
    try:
        ddf = data.to_dask_dataframe()[dimension_names]
    except ValueError as e:
        # check this is the chunking error
        if 'Object has inconsistent chunks' in str(e):
            logger.warning(f"Xarray had problems reading chunks from the clip of <ID={entry.id}>. Trying to rechunk the data...")
            unified = data.unify_chunks()
            ddf = unified.to_dask_dataframe()[dimension_names]
        else:
            # in any other case re-raise the error
            raise e

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

    return table_name


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

    return table_name


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

        # add the overview of aggregations to the metadata
        db.execute(AGGREGATION_VIEW)
        logger.info(f"duckdb - {AGGREGATION_VIEW}")

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


def add_temporal_integration(entry: Entry, table_name: str, database_path: Optional[str] = None, funcs: Optional[List[str]] = None) -> None:
    # if there is no temporal dimension, we cannot integrate
    if entry.datasource.temporal_scale is None:
        return 
    
    # check if we have a database path
    db_path = _get_database_path(database_path)

    # create a container for the aggregation statements
    aggr_statements = []

    # extract the time dimension and the variable names
    aggr_statements.append("date_trunc(precision, time) AS time")
    
    # build the aggregation statements for all variables
    if funcs is None:
        funcs = list(AGGREGATIONS.keys())
    
    # add every requested aggregation function for each variable
    for variable in entry.datasource.variable_names:
        for func in funcs:
            aggr_statements.append(AGGREGATIONS[func].format(variable=variable))
    
    # build the sql statement
    sql = f"CREATE MACRO {table_name}_temporal_aggregate(precision) AS TABLE SELECT {', '.join(aggr_statements)} FROM {table_name} GROUP BY date_trunc(precision, time);"
    logger.info(f"duckdb - {sql}")

    # connect to the database and run
    with duckdb.connect(database=db_path, read_only=False) as db:
        db.execute(sql)


def add_spatial_integration(entry: Entry, table_name: str, spatio_temporal: bool = False, database_path: Optional[str] = None, funcs: Optional[List[str]] = None, target_epsg: int = 3857, algin_cell: str = 'center') -> None:
    # if there is no spatial dimension, we cannot integrate
    if entry.datasource.spatial_scale is None:
        return
    
    if entry.datasource.temporal_scale is None and spatio_temporal:
        return

    # check if we have a database path
    db_path = _get_database_path(database_path)

    # load the variable names
    variable_names = entry.datasource.variable_names
    
    # define the time aggregator if we are spatio-temporal
    temporal_agg = 'date_trunc(precision, time) AS time, ' if spatio_temporal else ''
    # define the inner transform statement
    # TODO: DuckDB only supports 2D points, thus we need to check that here and build custom handling for z-dimensions
    if len(entry.datasource.spatial_scale.dimension_names) == 2:
        INNER = f"SELECT {temporal_agg} ST_Transform(ST_Point(lon, lat), 'epsg:4326', 'epsg:{target_epsg}') as geom, {', '.join(variable_names)} FROM {table_name}"
    else:
        raise NotImplementedError(f"Currently, non-2D spatial dimensions are not supported for spatial integration.")
        
    # create a container for the aggregation statements
    aggr_statements = []

    # build the aggregation statements for all variables
    if funcs is None:
        funcs = list(AGGREGATIONS.keys())
    
    # add every requested aggregation function for each variable
    for variable in entry.datasource.variable_names:
        for func in funcs:
            aggr_statements.append(AGGREGATIONS[func].format(variable=variable))

    # build the spatial aggregation statement
    # get the cell alignment function
    ALIGN = CellAlignFunc[algin_cell.lower()]
    
    # build the cell align sql statement
    SPATIAL_AGG = f"{ALIGN}(ST_Y(geom) / resolution)::int * resolution AS y, {ALIGN}(ST_X(geom) / resolution)::int * resolution AS x"
            
    #figure out the macro name
    if spatio_temporal:
        macro_name = f"{table_name}_spatiotemporal_aggregate"
        sql = f"CREATE MACRO {macro_name}(resolution, precision) AS TABLE WITH t as ({INNER}) SELECT time, {SPATIAL_AGG}, {', '.join(aggr_statements)} FROM t GROUP BY time, x, y;"
    else:
        macro_name = f"{table_name}_spatial_aggregate"
        sql = f"CREATE MACRO {macro_name}(resolution) AS TABLE WITH t as ({INNER}) SELECT {SPATIAL_AGG}, {', '.join(aggr_statements)} FROM t GROUP BY x, y;"
    # build the final sql statement
    
    logger.info(f"duckdb - {sql}")

    # connect to the database and run
    with duckdb.connect(database=db_path, read_only=False) as db:
        db.load_extension('spatial')
        db.execute(sql)

from typing import Optional, List, Dict, TypedDict
from collections import defaultdict
import time
from concurrent.futures import Executor

import duckdb
import polars as pl

from param import load_params
from logger import logger
from writer import dispatch_result_saver

class AggregationMacros(TypedDict):
    temporal: str
    spatial: str
    spatio_temporal: str


class LayerInfo(TypedDict):
    table_name: str
    variable: str
    id: int
    aggregations: AggregationMacros


def _get_database_path(database_path: Optional[str] = None) -> str:
    # check if we have a database path
    if database_path is None:
        params = load_params()
        database_path = str(params.database_path)
    
    # get the database name
    return database_path


def available_aggregations(database_path: Optional[str] = None) -> Dict[str, LayerInfo]:
    # check if we have a database path
    db_path = _get_database_path(database_path)

    # use the aggregations view to load the data
    with duckdb.connect(db_path, read_only=True) as db:
        df = db.sql("FROM aggregations;").df()
    
    # convert to json
    records = df.to_dict(orient='records')

    # build a default dict
    layers = defaultdict(dict)

    # iterate over the records
    for record in records:
        # get the table name
        table_name = record['data_table']

        # check if we already saved something for this data table
        if table_name in layers:
            layers[table_name]['aggregations'][record['aggregation_scale']] = record['function_name']
        else:
            layers [table_name] = {
                'id': record['id'],
                'vairable': record['variable'],
                'table_name': table_name,
                'aggregations': {
                    record['aggregation_scale']: record['function_name']
                }
            } 
    
    # return the layers
    return layers


def run_aggregation(table_name: str, aggregation_scale: str, precision: str = 'day', resolution: int = 1000, layers: Optional[Dict[str, LayerInfo]] = None, database_path: Optional[str] = None) -> pl.DataFrame:
    # check if we have a database path
    db_path = _get_database_path(database_path)

    # first check if layers was given
    if layers is None:
        layers = available_aggregations(database_path=db_path)
    
    # check if the table name and aggregation_scale is in the layers
    try:
        macro = layers[table_name]['aggregations'][aggregation_scale]
    except KeyError:
        raise ValueError(f"The dataset {db_path} does not contain a aggregation MACRO for {aggregation_scale} aggregations on table {table_name}")
    
    # connect and run the aggregation query
    with duckdb.connect(db_path, read_only=True) as db:
        db.load_extension('spatial')
        # TODO: here we could add pagination

        # build the query
        if aggregation_scale == 'temporal':
            sql = f"FROM {macro}('{precision}');"
        elif aggregation_scale == 'spatial':
            sql = f"FROM {macro}({resolution});"
        elif aggregation_scale == 'spatiotemporal':
            sql = f"FROM {macro}({resolution}, '{precision}');"
        else:
            raise AttributeError(f"Unknown aggregation scale {aggregation_scale}")
        
        # start the timer 
        t1 = time.time()
        df = db.sql(sql).pl()
        t2 = time.time()

        # logging
        logger.info(f"duckdb - {sql}")
        logger.info(f"took {t2 - t1:.2f} seconds")

        # return the dataframe
        return df
        

def aggregate_scale(aggregation_scale: str, executor: Executor, precision: Optional[str] = None, resolution: Optional[int] = None, database_path: Optional[str] = None) -> None:
    # check if we have a database path
    db_path = _get_database_path(database_path)

    # load parameters
    params = load_params()

    # load the layers
    layers = available_aggregations(database_path=db_path)

    # get the parameters
    resolution = resolution or load_params().resolution
    precision = precision or load_params().precision

    # create a container for 'mean' variables
    means = None

    # iterate over the layers
    for layer in layers.keys():
        # do the aggregation
        try:
            df = run_aggregation(table_name=layer, aggregation_scale=aggregation_scale, precision=precision, resolution=resolution, layers=layers, database_path=db_path)
        except Exception as e:
            logger.error(str(e))
            continue

        # dispatch saving
        path = params.result_path / f"{layer}_{aggregation_scale}_aggs.parquet"
        dispatch_result_saver(file_name=str(path), data=df, executor=executor)

        # get the join statement based on aggregation scale
        if aggregation_scale == 'temporal':
            on = ['time']
        elif aggregation_scale == 'spatial':
            on = ['lon', 'lat']
        elif aggregation_scale == 'spatiotemporal':
            on = ['time', 'lon', 'lat']
        
        # extract only the index and column 'mean'
        mean = df[[*on, 'mean']].rename({'mean': layer})

        # join the means
        if means is None:
            means = mean
        else:
            means = means.join(mean, on=on, how='outer')
    
    # finally save the means
    path = params.result_path / f"mean_{aggregation_scale}_aggs.parquet"
    dispatch_result_saver(file_name=str(path), data=means, executor=executor)

    # done
        


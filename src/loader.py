from typing import Union, Optional, List
from pathlib import Path
from datetime import datetime
import json
import glob
import tempfile
import subprocess
import time

from metacatalog.models import Entry
import rioxarray
import xarray as xr
import pandas as pd
import geopandas as gpd

import shapely
from pyproj import CRS

from logger import logger



# Maybe this function becomes part of metacatalog core or a metacatalog extension
def load_entry(entry: Entry, start: Optional[datetime] = None, end: Optional[datetime] = None, reference_area = None):
    # 1. get the path to the datasource
    path_type = entry.datasource.type.name

    # TODO: here we need to load the axis names from the datasource
    time_axis = None
    spatial_axis = None

    # if the type is internal or external, we need to use the load_sql_source
    if path_type in ('internal', 'external'):
        data = load_sql_source(entry, start=start, end=end)
    # TODO: here we can be explicite about data source types
    else:
        data = load_file_source(entry, start=start, end=end, time_axis=time_axis, reference_area=reference_area)
    
    # For now return here
    return data


def load_sql_source(entry: Entry, start: Optional[datetime] = None, end: Optional[datetime] = None):
    # if the source is external, we can't use it right now, as it is not clear
    # yet if the Datasource.path is the connection or the path inside the database
    if entry.datasource.type.name == 'external':
        raise NotImplementedError("External database datasources are not supported yet.")
    
    # we are internal and can request a session to the database
    # TODO: now this has to be replaced by the new logic where every entry goes into its own table
    data = entry.get_data(start=start, end=end)

    return data


def load_http_source(entry: Entry, start: Optional[datetime] = None, end: Optional[datetime] = None):
    raise NotImplementedError("HTTP datasources are not supported yet.")


def load_file_source(entry: Entry, start: Optional[datetime] = None, end: Optional[datetime] = None, time_axis: Optional[str] = None, reference_area = None) -> Union[xr.Dataset, dict]:
    # create a Path from the name
    name = entry.datasource.path
    path = Path(name)

    # go for the different suffixes
    if path.suffix.lower() in ('.nc', '.netcdf', '.cdf', 'nc4'):
        out_path = load_netcdf_file(name, time_axis=time_axis, start=start, end=end, reference_area=reference_area)

        # load the data into a single nc file and to a parquet file
        data = merge_multi_file_netcdf(entry=entry, path=out_path, save_nc=True, save_parquet=False)
        
        # return the dataset
        return data


def load_netcdf_file(name: str, time_axis: Optional[str] = None, start: Optional[datetime] = None, end: Optional[datetime] = None, reference_area: Optional[dict] = None) -> str:
    # convert reference area to shapely
    ref = shapely.from_geojson(json.dumps(reference_area))
    bnd = ref.bounds

    # check if there is a wildcard in the name
    if '*' in name:
        fnames = glob.glob(name)
    else:
        fnames = [name]
    
    # check the amount of files to be processed
    if len(fnames) > 1:
        logger.debug(f"For {name} found {len(fnames)} files.")
    elif len(fnames) == 0:
        logger.warning(f"Could not find any files for {name}.")
        return None
    else:
        logger.debug(f"Resource {name} is single file.")

    # get a temporary directory
    # TODO: use a deterministic path here, in order to be reproducible
    out_path = Path(tempfile.mkdtemp())

    # preprocess each netcdf / grib / zarr file
    for fname in fnames:
        # read the min and max time and check if we can skip
        ds = xr.open_dataset(fname)

        # check if we there is a time axis
        try:
            if time_axis is None:
                time_axis = [c for c in ds.coords if c.lower() in ('tstamp', 'time', 'date', 'datetime')][0]

            # get the min and max time
            min_time = pd.to_datetime(ds[time_axis].min().values)
            max_time = pd.to_datetime(ds[time_axis].max().values)

            if (start is not None and start > max_time.tz_localize(start.tzinfo)) or (end is not None and end < min_time.tz_localize(end.tzinfo)):
                logger.info(f'skipping {fname} as it is not in the time range: {start} - {end}')
                continue
        except IndexError:
            logger.warning(f"The dataset {fname} does not contain a datetime coordinate.")
        
        # close datasource again
        ds.close()
        
        # run CDO to clip the data
        out_name = out_path / Path(fname).name
        sel_cmd = f'-sellonlatbox,{bnd[0]},{bnd[2]},{bnd[1]},{bnd[3]}'

        # TODO if the we did not skip, but start or end in WITHIN the time range, we can search the indices to use
        # and add a -seltimestep command to the sel_cmd
        # alternatively we use the duckdb path and do the slicing there
        
        # run the CDO select command
        t1 = time.time()
        p = subprocess.run(['cdo', sel_cmd, fname, out_name], stdout=subprocess.PIPE, text=True)
        t2 = time.time()
        
        # use the logger to log the output
        logger.info(f"cdo {sel_cmd} {fname} {out_name}")
        
        # check if CDO had output
        logger.info(p.stdout)
        logger.debug(f"took {t2-t1:.2f} seconds")
    
    # return the out_path
    return out_path


def merge_multi_file_netcdf(entry: Entry, path: str, save_nc: bool = True, save_parquet: bool = True) -> pd.DataFrame:
    # check if this file should be saved
    if save_nc:
        out_name = f'/out/{entry.variable.name.replace(" ", "_")}_{entry.id}.nc'
    else:
        out_name = f'{path}/merged.nc'

    # build the CDO command
    merge_cmd = ['cdo', 'mergetime', str(Path(path) / '*.nc'), out_name]
    
    # run merge command
    t1 = time.time()
    subprocess.run(merge_cmd)
    t2 = time.time()
    logger.info(' '.join(merge_cmd))
    logger.info(f"took {t2-t1:.2f} seconds")

    # open the merged data
    # TODO infer time_axis from the entry and figure out a useful time_axis chunk size here
    data = xr.open_dataset(out_name, decode_coords=True, chunks={'time': 1})

    if not save_parquet:
        return data
    
    # TODO: put this into an extra STEP
    # TODO: figure out axis_names from the entry here THIS IS NOT REALLY USEFULL
    time_axis = next(([_] for _ in ('tstamp', 'time', 'date', 'datetime') if _ in data.coords), [])
    x_axis = next(([_] for _ in ('lon', 'longitude',  'x') if _ in data.coords), [])
    y_axis = next(([_] for _ in ('lat', 'latitude', 'y') if _ in data.coords), [])
    var_name = [_ for _ in ('pr', 'hurs', 'tas', 'rsds', 'tasmin', 'tasmax') if _ in data.data_vars]
    variable_names = [*time_axis, *x_axis, *y_axis, *var_name]

    # convert to long format
    t1 = time.time()
    df = data[var_name].to_dask_dataframe()[variable_names]
    t2 = time.time()
    logger.debug(f"Converting {out_name} to long format in {t2-t1:.2f} seconds.")

    return df


def clip_netcdf_file(entry: Entry, data: xr.Dataset, reference_area: dict) -> xr.Dataset:
    # convert reference area to GeoDataFrame
    df = gpd.GeoDataFrame.from_features([reference_area])

    # get the source name
    name = f"{entry.datasource.path} <ID={entry.id}>"

    # convert to coordinate reference system
    if data.rio.crs is None:
        logger.warning(f"The netCDF {name} does not contain a coordinate reference system. This is bad. Trying to figure something out.")

        # create a dict of the data_vars
        vars = {name: data[name].attrs for name in data.data_vars if 'crs' in name.lower()}
        logger.debug(f"Found {len(vars)} canidates for CRS in {name} variable attributes.")
        
        # filter for the first occurence of spatial_ref
        attr_name, attr = next(((n, vars[n]['spatial_ref']) for n in vars if 'spatial_ref' in vars[n]), (None, None))
        
        # TODO: This is a really dirty fix. 
        if attr_name is not None:
            crs = CRS.from_wkt(attr)
            data.rio.set_crs(crs, inplace=True)
            logger.info(f"Used {attr_name} to infer CRS {crs.name} for {name}.")

        else:
            logger.warning(f"Could not find a CRS for {name}. Skip cropping.")
            return data
    
    # clip the data
    try:
        clip = data.rio.clip(df.geometry.values, crs=4326, drop=True)
        logger.info(f"Clipped {name} to reference_area.")

        return clip
    except Exception as e:
        # TODO: if there is an error, fall back to duckdb solution
        logger.exception(f"SKIP clipping on {name} as an exception occured.")
        return data


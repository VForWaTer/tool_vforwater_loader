from typing import Union, Optional
from pathlib import Path
from datetime import datetime
import warnings
import json

from metacatalog.models import Entry
import rioxarray
import xarray as xr
import pandas as pd
import rasterio as rio


# Maybe this function becomes part of metacatalog core or a metacatalog extension
def load_entry(entry: Entry, start: Optional[datetime] = None, end: Optional[datetime] = None, reference_area = None):
    # 1. get the path to the datasource
    path_type = entry.datasource.type.name
    data_path = entry.datasource.path

    # TODO: here we need to load the axis names from the datasource
    time_axis = None
    spatial_axis = None

    # if the type is internal or external, we need to use the load_sql_source
    if path_type in ('internal', 'external'):
        data = load_sql_source(entry, start=start, end=end)
    # TODO: here we can be explicite about data source types
    else:
        data = load_file_source(data_path, start=start, end=end, time_axis=time_axis)

    # 3. clip the reference area

    
    # - unify the resolution - we have no strategy here yet
    
    # 4. save the dataset
    # create the out name
    out_name = f"/out/{entry.variable.name.replace(' ', '_')}_{entry.id}"
    if isinstance(data, xr.Dataset):
        fname = f"{out_name}.nc"
        print('start writing')
        data.load()
        data.to_netcdf(fname)
        print('done writing')
    
    elif isinstance(data, pd.DataFrame):
        fname = f"{out_name}.parquet"
        data.to_parquet(fname)
    
    elif isinstance(data, rio.DatasetReader):
        fname = f"{out_name}.tif"
        with rio.open(fname, 'w', **data.profile) as dst:
            dst.write(data.read())
    
    # 5. create the metadata
    with open(f"{out_name}.json", 'w') as f:
        # TODO: here we need the pydantic models to do that better
        json.dump(entry.to_dict(deep=True, stringify=True), f, indent=4)

    return fname


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


def load_file_source(name: str, start: Optional[datetime] = None, end: Optional[datetime] = None, time_axis: Optional[str] = None) -> Union[xr.Dataset, dict]:
    # create a Path from the name
    path = Path(name)

    # go for the different suffixes
    if path.suffix.lower() in ('.nc', '.netcdf', '.cdf', 'nc4'):
        # check if there is a wildcard in the name
        if '*' in name:
            data = xr.open_mfdataset(name, combine='by_coords', mask_and_scale=True, decode_coords=True, lock=False, chunks='auto')
        else:
            data = xr.open_dataset(name, decode_coords=True, mask_and_scale=True)
        
        # check if the time axis is present
        if start is not None or end is not None:
            # find the first datetime coordinate is no axis is specified
            if time_axis is None:
                try:
                    # TODO: replace by a time_axis name in metacatalog
                    time_axis = [c for c in data.coords if c.lower() in ('tstamp', 'time', 'date', 'datetime')][0]
                except IndexError:
                    warnings.warn(f"The dataset {name} does not contain a datetime coordinate.")
                    time_axis = False
            
            # do the slicing
            if start is not None and time_axis:
                data = data.where(data[time_axis] >= pd.to_datetime(start).tz_localize(None), drop=True)
            if end is not None and time_axis:
                data = data.where(data[time_axis] <= pd.to_datetime(end).tz_localize(None), drop=True)
        
        # return the dataset
        return data


def clip_xrarray_file():
    pass

def clip_rasterio_file():
    pass
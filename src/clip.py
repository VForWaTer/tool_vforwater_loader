from typing import Union
import time

from metacatalog.models import Entry
import geopandas as gpd
import rioxarray
import xarray as xr
from pyproj import CRS
from geocube.api.core import make_geocube

from logger import logger
from writer import dispatch_save_file

def reference_area_to_file(reference_area: dict) -> str:
    # create a geodataframe
    df = gpd.GeoDataFrame.from_features([reference_area])

    # save the reference area as a geojson file
    path = '/out/reference_area.geojson'
    df.to_file(path, driver='GeoJSON')

    return path

def infer_crs_from_netcdf(data: xr.Dataset) -> CRS:
    # try to infer from rioxarray
    if data.rio.crs is not None:
        return data.rio.crs
    
    # if we are still here, try to infer from attributes
    crs_wkt = next((v for k, v in data.attrs.items() if 'crs' in k.lower()), None)
    if crs_wkt is not None:
        crs = CRS.from_wkt(crs_wkt)
        logger.debug(f"Found CRS in attributes: {crs.name}")
        return crs
    
    # if we are still here, try to infer from the attributes of a data variable encoding the CRS
    crs_wkt = next((data[var].attrs.get('spatial_ref') for var in data.data_vars if 'crs' in var.lower()), None)
    if crs_wkt is not None:
        crs = CRS.from_wkt(crs_wkt)
        logger.debug(f"Found CRS in data variable attributes: {crs.name}")
        return crs
    
    # no idea left
    return False


def mask_xarray_dataset(entry: Entry, path_or_data: Union[str, xr.Dataset], reference_path: str = '/out/reference_area.geojson') -> xr.Dataset:
    # first open the dataset
    if isinstance(path_or_data, str):
        # TODO:  we need a strategy for the chunking here
        data = xr.open_dataset(path_or_data, mask_and_scale=True, decode_coords=True, chunks={'time': 1})
    else:
        data = path_or_data

    # check if we need to infer the CRS
    if data.rio.crs is None:
        logger.warning(f"Datasource for <ID={entry.id}> has no CRS. Trying to infer from attributes and data variable encoding. Please fix the datasource.")
        crs = infer_crs_from_netcdf(data)
        if crs:
            data = data.rio.set_crs(crs, inplace=True)
        else:
            logger.error(f"The xarray.Dataset build for source <ID={entry.id}> has no CRS, and none could be inferred from attributes and data variable encoding. Skipping masking.")
            return data
    
    # build a geocube of the reference area
    df = gpd.read_file(reference_path, driver='GeoJSON')

    # add a oid
    if 'oid' not in df.columns:
        df['oid'] = 1

    # build a geocube
    logger.debug(f"Building a geocube of for source <ID={entry.id}> with a reference area mask...")
    try:
        cube = make_geocube(
            vector_data=df,
            measurements=['oid'],
            like=data
        ) 
    except Exception as e:
        logger.exception(f"Could not build a geocube for source <ID={entry.id}>.")
        return data
    
    # copy the source files
    cube.attrs = data.attrs
    for var in data.data_vars:
        cube[var] = data[var]

    # mask the data
    t1 = time.time()
    cube_mask = cube.where(cube.oid == 1, drop=True).copy()
    t2 = time.time()
    logger.debug(f"Masking geocube for source <ID={entry.id}> took {t2-t1:.2f} seconds.")
    
    return cube_mask

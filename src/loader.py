import glob
import subprocess
import time
from concurrent.futures import Executor
from pathlib import Path

from metacatalog.models import Entry
import rioxarray
import xarray as xr
import pandas as pd
import geopandas as gpd
import rasterio as rio

from logger import logger
from writer import dispatch_save_file, entry_metadata_saver
from param import load_params, Params
from utils import whitebox_log_handler

from WBT.whitebox_tools import WhiteboxTools

# Maybe this function becomes part of metacatalog core or a metacatalog extension
def load_entry_data(entry: Entry, executor: Executor) -> str:
    # 1. get the path to the datasource
    path_type = entry.datasource.type.name

    # if the type is internal or external, we need to use the load_sql_source
    if path_type in ('internal', 'external'):
        data_path = load_sql_source(entry, executor=executor)
    # TODO: here we can be explicite about data source types
    else:
        data_path = load_file_source(entry, executor=executor)

    # Return the data path to the entry-level dataset
    return data_path


def load_sql_source(entry: Entry, executor: Executor) -> str:
    # load the params
    params = load_params()

    # if the source is external, we can't use it right now, as it is not clear
    # yet if the Datasource.path is the connection or the path inside the database
    if entry.datasource.type.name == 'external':
        raise NotImplementedError("External database datasources are not supported yet.")
    
    # we are internal and can request a session to the database
    # TODO: now this has to be replaced by the new logic where every entry goes into its own table
    data = entry.get_data(start=params.start_date, end=params.end_date)

    # dispatch a save task for the data
    target_name = f"{entry.variable.name.replace(' ', '_')}_{entry.id}"
    dispatch_save_file(entry=entry, data=data, executor=executor, base_path=str(params.dataset_path), target_name=target_name)
    return target_name


def load_http_source(entry: Entry):
    raise NotImplementedError("HTTP datasources are not supported yet.")


def load_file_source(entry: Entry, executor: Executor) -> str:
    # create a Path from the name
    name = entry.datasource.path
    path = Path(name)

    # debug the path
    logger.debug(f"Metacatalog entry datasource path: {path}; exists: {path.exists()}")

    # go for the different suffixes

    if path.suffix.lower() in ('.nc', '.netcdf', '.cdf', 'nc4'):
        logger.info("load_file_source identified a netCDF file and will now process it.")
        # load the netCDF file time & space chunks to the output folder
        out_path = load_netcdf_file(entry, executor=executor)
        
        # return the dataset
        return out_path
    elif path.suffix.lower() in ('.tif', '.tiff', '.dem'):
        logger.info("load_file_source identified a raster file and will now process it.")
        out_path = load_raster_file(entry, executor=executor)
    else:
        logger.warning(f"Loading a file source was requested, but the passed file extension '{path.suffix.lower()}' is not recognized.")


def load_netcdf_file(entry: Entry, executor: Executor) -> str:
    # load the params
    params = load_params()

    # get the file name
    name = entry.datasource.path

    # check if there is a wildcard in the name
    msg = f"Entry <ID={entry.id}> supplied the raw entry.datasource.path={name}."
    if '*' in name:
        fnames = glob.glob(name)
        msg += f" Has a wildcard, resolved path to {len(fnames)} files: [{fnames}]."
    else:
        fnames = [name]
        msg += " Resource is a single file."
    
    # check the amount of files to be processed
    if len(fnames) == 0:
        logger.warning(msg + f" Could not find any files for {name}.")
        return None
    else:
        logger.debug(msg)

    # get the time axis
    temporal_dims = entry.datasource.temporal_scale.dimension_names if entry.datasource.temporal_scale is not None else []
    
    # get a path for the current dataset path
    dataset_base_path = params.dataset_path / f"{entry.variable.name.replace(' ', '_')}_{entry.id}"

    # preprocess each netcdf / grib / zarr file
    for i, fname in enumerate(fnames):
        # read the min and max time and check if we can skip
        ds = xr.open_dataset(fname, decode_coords='all', mask_and_scale=True)

        # check if we there is a time axis
        if len(temporal_dims) > 0:
            # get the min and max time
            min_time = pd.to_datetime(ds[temporal_dims[0]].min().values)
            max_time = pd.to_datetime(ds[temporal_dims[0]].max().values)

            if (
                params.start_date is not None and params.start_date > max_time.tz_localize(params.start_date.tzinfo)
            ) or (
                params.end_date is not None and params.end_date < min_time.tz_localize(params.end_date.tzinfo)
            ):
                logger.debug(f'skipping {fname} as it is not in the time range: {params.start_date} - {params.end_date}')
                ds.close()
                continue
        else:
            ds.close()
            logger.warning(f"The dataset {fname} does not contain a datetime coordinate.")
        
        # this does not work for ie HYRAS netCDF files
        if params.netcdf_backend == 'cdo':
            ds.close()
            path = _clip_netcdf_cdo(fname, params)
            return path
        
        elif params.netcdf_backend == 'xarray':
            data = _clip_netcdf_xarray(entry, fname, ds, params)

        elif params.netcdf_backend == 'parquet':
            # use the xarray clip first
            ds = _clip_netcdf_xarray(entry, fname, ds, params)

            data = ds.to_dask_dataframe()[entry.datasource.dimension_names].dropna()
        
        # if we are still here, dispatch the save task for intermediate file chunk
        # we do not need the future here, we can directly move to the next file
        
        # as we write many files in parallel here, we need to provide the target names one-by-one
        # and supress the creation of metadata files
        dataset_base_path.mkdir(parents=True, exist_ok=True)
        
        # get the filenmae
        filename = f"{entry.variable.name.replace(' ', '_')}_{entry.id}"
        target_name = f"{filename}_part_{i + 1}.nc"

        dispatch_save_file(entry=entry, data=data, executor=executor, base_path=str(dataset_base_path), target_name=target_name, save_meta=False)
        # if there are many files, we save the metadata only once
        if i == 0:
            metafile_name = str(params.dataset_path / f"{filename}.metadata.json")
            entry_metadata_saver(entry, metafile_name)
            logger.info(f"Saved metadata for dataset <ID={entry.id}> to {metafile_name}.")

    # return the out_path
    return str(dataset_base_path)


def _clip_netcdf_cdo(path: Path, params: Params):
    # get the output name
    out_name = params.intermediate_path / path.name

    # build the several commands
    ref = params.reference_area_df
    bnd = ref.geometry[0].bounds

    # create the lonlatbox command
    lonlat_cmd = f"-sellonlatbox,{bnd[0]},{bnd[2]},{bnd[1]},{bnd[3]}"

    # create the selregion command
    selregion_cmd = f"-selregion,{params.base_path}/reference_area.ascii"

    # build the full command
    cmd = ['cdo', selregion_cmd, lonlat_cmd, str(path), str(out_name)]
    
    # run
    t1 = time.time()
    subprocess.run(cmd)
    t2 = time.time()

    # log the command
    logger.info(' '.join(cmd))
    logger.info(f"took {t2-t1:.2f} seconds")
    
    return str(out_name)


def _clip_netcdf_xarray(entry: Entry, file_name: str, data: xr.Dataset, params: Params):
    if data.rio.crs is None:
        logger.error(f"Could not clip {data} as it has no CRS.")
        
        # TODO: how to handle this case?
        return data
    else:
        # inform the user that we are processing the file using xarray
        logger.info(f"Processing {file_name} in python using rioxarray and xarray (source <ID={entry.id}>)...")
    
    # start a timer
    t1 = time.time()

    # extract only the data variable
    # TODO maybe activate this via a parameter?
    # variable_names = entry.datasource.variable_names
    # ds = data[variable_names].copy()
    # logger.info(f"python - ds = data[{variable_names}].copy()")
    ds = data

    # first go for the lonlatbox clip
    ref = params.reference_area_df
    bounds = ref.geometry[0].bounds

    # then the region clip
    if entry.datasource.temporal_scale is not None:
        time_dim = entry.datasource.temporal_scale.dimension_names[0]
        ds.chunk({entry.datasource.temporal_scale.dimension_names[0]: 'auto'})

        logger.info(f"python - ds.chunk{{'{time_dim}': 'auto'}})")
    else:
        time_dim = None

    # do the lonlat and then the region clip
    lonlatbox = ds.rio.clip_box(*bounds, crs=4326)
    region = lonlatbox.rio.clip([ref.geometry[0]], crs=4326, all_touched=params.cell_touches)

    # log out
    logger.info(f"python - lonlatbox df.rio.clip_box(({','.join([str(_) for _ in bounds])}), crs=4326)")
    logger.info(f"python - region = lonlatbox.rio.clip([ref.geometry[0]], crs=4326, all_touched={params.cell_touches})")

    # do the time clip
    if time_dim is not None:        
        # TODO: check the attrs of time_dim to see if there is timezone information

        # convert params to UTC as we assume any xarray souce to use UTC dates
        time_slice = slice(
            pd.to_datetime(params.start_date).tz_convert('UTC').tz_localize(None) if params.start_date is not None else None, 
            pd.to_datetime(params.end_date).tz_convert('UTC').tz_localize(None) if params.end_date is not None else None
        )

        # subset the time axis
        region = region.sel(**{time_dim: time_slice})
        logger.info(f"python - region.sel({time_dim}=slice({time_slice.start}, {time_slice.stop}))")
    
    t2 = time.time()
    logger.info(f"took {t2-t1:.2f} seconds")

    # return the new dataset
    return region


def load_raster_file(entry: Entry, executor: Executor) -> str:
    # load the params
    params = load_params()

    # get the reference area
    reference_area = params.reference_area_df

    # get a path for the current dataset path
    dataset_base_path = params.dataset_path / f"{entry.variable.name.replace(' ', '_')}_{entry.id}"

    # create the base path
    dataset_base_path.mkdir(parents=True, exist_ok=True)

    # get the file name from the source
    source_file_name = entry.datasource.path
    source_path = Path(source_file_name)
    
    # figure out if there is a * in the name, or the name is a directory
    if '*' in source_file_name:
        names = glob.glob(source_file_name)
    elif source_path.is_dir():
        names = [str(name) for name in source_path.glob('*')]
    else:
        names = [source_file_name]
    
    # filter
    fnames = [name for name in names if Path(name).suffix.lower() in ('.tif', '.tiff', '.dem')]
    
    # info
    logger.info(f"Exploded the final list of raster tiles to : [{fnames}]")

    # define an error handler
    def error_handler(future):
        exc = future.exception()
        if exc is not None:
            logger.error(f"ERRORED: clipping dataset <ID={entry.id}>: {str(exc)}")
    
    # collect all futures
    futures = []
    # go for each file
    for i, fname in enumerate(fnames):
        # derive an out-name
        out_name = None if len(fnames) == 1 else f"{Path(fname).stem}_part_{i + 1}.tif"
        # submit each save task to the executor
        future = executor.submit(_rio_clip_raster, fname, reference_area, dataset_base_path, out_name=out_name, touched=params.cell_touches)
        future.add_done_callback(error_handler)
        futures.append(future)
    
    # wait until all are finished
    tiles = [future.result() for future in futures if future.result() is not None]
    
    # run the merge function and delete the other files
    if len(tiles) > 1:
        logger.debug('Starting WhitboxTools mosaic operation...')
        _wbt_merge_raster(dataset_base_path, f"{entry.variable.name.replace(' ', '_')}_{entry.id}.tif")

        # remove the tiles
        for tile in tiles:
            Path(tile).unlink()
    
    # check if there is exactly one tile
    elif len(tiles) == 1:
        # rename the file
        new_name = dataset_base_path / f"{entry.variable.name.replace(' ', '_')}_{entry.id}.tif"
        Path(tiles[0]).rename(new_name)
        tiles = [str(new_name)]
    else:
        logger.warning(f'No tiles were clipped for the reference area. It might not be covered by dataset <ID={entry.id}>')

    # save the metadata
    metafile_name = str(params.dataset_path / f"{entry.variable.name.replace(' ', '_')}_{entry.id}.metadata.json")
    entry_metadata_saver(entry, metafile_name)
    logger.info(f"Saved metadata for dataset <ID={entry.id}> to {metafile_name}.")

    # some logging
    return str(dataset_base_path)
    

def _rio_clip_raster(file_name: str, reference_area: gpd.GeoDataFrame, base_path: Path, out_name: str = None, touched: bool = False):
    t1 = time.time()

    # open the raster file using rasterio
    with rio.open(file_name, 'r') as src:
        # do the masking
        try:
            out_raster, out_transform = rio.mask.mask(src, reference_area.geometry, crop=True, all_touched=touched, nodata=src.nodata)
        except ValueError as e:
            if 'Input shapes do not overlap raster' in str(e):
                logger.debug(f"Skipping {file_name} as it does not overlap with the reference area.")
                return None
            else:
                logger.exception(f"An unexpected error occured: {str(e)}")

        # save the out meta
        out_meta = src.meta.copy()

        # update the metadata
        out_meta.update({
            "height": out_raster.shape[1],
            "width": out_raster.shape[2],
            "transform": out_transform,
            "nodata": src.nodata
        })

    # finally save the raster
    if out_name is None:
        out_path = base_path / Path(file_name).name
    else:
        out_path = base_path / out_name

    with rio.open(str(out_path), 'w', **out_meta) as dst:
        dst.write(out_raster)
    
    t2 = time.time()
    logger.info(f"Clipped {file_name} to {out_path} in {t2-t1:.2f} seconds.")

    # return the output path
    return str(out_path)

def _wbt_merge_raster(input_folder: Path, out_name: str):
    # initialize the whitebox tools
    wbt = WhiteboxTools()
    wbt.set_verbose_mode(True)

    # this could be mirrored in the params
    wbt.set_compress_rasters(True)

    # set whitebox path to the newly created folder
    wbt.set_working_dir(str(input_folder))

    # run the mosaic tool on the raster source
    wbt.mosaic(output=out_name, method="nn", callback=whitebox_log_handler)
    
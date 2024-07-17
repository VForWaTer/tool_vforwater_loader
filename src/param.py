"""
translate all params from the tools.yml and input.json into a pydantic model
this will make access to the variables easier across many submodules

This could be a general pattern for json2args. We would need a factory function
that consumes the yml to build the model and uses the inputs.json to instantiate it
"""
from typing import List
from datetime import datetime
from pathlib import Path
import tempfile
from enum import Enum

from pydantic import BaseModel, Field
import geopandas as gpd


# create the Enum for integration type
class Integrations(str, Enum):
    TEMPORAL = 'temporal'
    SPATIAL = 'spatial'
    SPATIO_TEMPORAL = 'spatiotemporal'
    ALL = 'all'
    NONE = 'none'
    FULL = 'full'


class NetCDFBackends(str, Enum):
    XARRAY = 'xarray'
    CDO = 'cdo'
    PARQUET = 'parquet'


class Params(BaseModel):
    # mandatory inputs are the dataset ids and the reference area
    dataset_ids: List[int]
    reference_area: dict = Field(repr=False)

    # optional parameters to configure the processing
    start_date: datetime = None
    end_date: datetime = None
    integration: Integrations = Integrations.ALL
    apply_aggregation: bool = False

    # optional parameter to configure output
    keep_data_files: bool = True
    database_name: str = 'dataset.duckdb'

    # optional parameter to provide result output
    precision: str = 'day'
    resolution: int = 5000
    cell_touches: bool = True

    # stuff that we do not change in the tool
    base_path: str = '/out'
    netcdf_backend: NetCDFBackends = NetCDFBackends.XARRAY

    # duckdb settings
    use_spatial: bool = False

    @property
    def dataset_path(self) -> Path:
        if self.keep_data_files:
            p = Path(self.base_path) / 'datasets'
        else:
            p = Path(tempfile.mkdtemp())
        
        # make the directory if it does not exist
        p.mkdir(parents=True, exist_ok=True)

        # return the path
        return p
    
    @property
    def result_path(self) -> Path:
        # create the results path if it does not exist
        p = Path(self.base_path) / 'results'
        p.mkdir(parents=True, exist_ok=True)

        return p
    
    @property
    def database_path(self) -> Path:
        return Path(self.base_path) / self.database_name

    @property
    def reference_area_df(self) -> gpd.GeoDataFrame:
        return gpd.GeoDataFrame.from_features([self.reference_area])


# manage a single instance to this class
__SINGLETON: Params = None
def load_params(**kwargs) -> Params:
    global __SINGLETON
    # create if needed
    if __SINGLETON is None:
        __SINGLETON = Params(**kwargs)
    
    # return
    return __SINGLETON
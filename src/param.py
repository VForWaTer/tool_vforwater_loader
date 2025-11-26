"""
translate all params from the tools.yml and input.json into a pydantic model
this will make access to the variables easier across many submodules

This could be a general pattern for json2args. We would need a factory function
that consumes the yml to build the model and uses the inputs.json to instantiate it
"""

import tempfile
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List

import geopandas as gpd
from pydantic import BaseModel, Field


class NetCDFBackends(str, Enum):
    XARRAY = "xarray"
    CDO = "cdo"
    PARQUET = "parquet"


class Params(BaseModel):
    # mandatory inputs are the dataset ids and the reference area
    dataset_ids: list[int]

    # optional parameters to configure the processing
    reference_area: dict | None = Field(repr=False, default=None)
    start_date: datetime = None
    end_date: datetime = None
    cell_touches: bool = True

    # stuff that we do not change in the tool
    base_path: str = "/out"
    dataset_folder_name: str = "datasets"
    netcdf_backend: NetCDFBackends = NetCDFBackends.XARRAY

    @property
    def dataset_path(self) -> Path:
        # set the databsets path
        p = Path(self.base_path) / self.dataset_folder_name

        # make the directory if it does not exist
        p.mkdir(parents=True, exist_ok=True)

        # return the path
        return p

    @property
    def result_path(self) -> Path:
        # create the results path if it does not exist
        p = Path(self.base_path) / "results"
        p.mkdir(parents=True, exist_ok=True)

        return p

    @property
    def reference_area_df(self) -> gpd.GeoDataFrame:
        return gpd.GeoDataFrame.from_features([self.reference_area])

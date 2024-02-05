# MetaCatalog Data Loader

[![Docker Image CI](https://github.com/VForWaTer/tool_vforwater_loader/actions/workflows/docker-image.yml/badge.svg)](https://github.com/VForWaTer/tool_vforwater_loader/actions/workflows/docker-image.yml)
[![DOI](https://zenodo.org/badge/743434463.svg)](https://zenodo.org/doi/10.5281/zenodo.10513947)

This is a containerized Python tool to use [MetaCatalog](https://github.com/vforwater/metacatalog), a metadata and data source
catalog to download data. The data is requested for a spatial and temporal extent, as specified in the metadata catalog, to be
consistent across scales. An implementation for this tool is the [V-FOR-WaTer platform](https://portal.vforwater.de).

This tool follows the [Tool Specification](https://vforwater.github.io/tool-specs/) for reusable research software using Docker.

## Description

[MetaCatalog](https://github.com/vforwater/metacatalog) stores metadata about internal and external datasets along with
information about the data sources and how to access them. Using this tool, one can request datasets (called *entries* in MetaCatalog) by their **id**. Additionally, an area of interest is supplied as a GeoJSON feature, called **reference area**.

The tool involves three main processing steps, of which only the first one is mandatory.

1. The database of the connected MetaCatalog instance is queried for the `dataset_ids`. The data-files are reuqested for 
the temporal extent of `start_date` and `end_date` if given, while the spatial extent is requested for the bounding box
of `reference_area`. MetaCatalog entires without either of the scales defined are loaded entierly.
Finally, the spatial extent is clipped by the `reference_area` to match exactly. Experimental parameters are not yet 
exposed, but involve:
    - `netcdf_backend`, which can be either `'CDO'` or `'xarray'` (default) can switch the software used for the clip
    of NetCDF data sources, which are commonly used for spatio-temporal datasets.
    - `touches` is a boolean that is `false` by default and configures if areal grid cells are considered part of 
    `reference_area` if they touch (`touches=true`) or only contain the grid center (`touches=false`).
All processed data-files for each source are then saved to `/out/datasets/`, while multi-file sources are saved to
child repositories. The file (or folder) names are built like: `<variable_name>_<entry_id>`.

2. The second step is only performed if the parameter `integration` is **not** set to `none`.
All available data sources are converted to long-format, where each atomic data value is indexed by the value of the
axes, that form the spatial and temporal scales (if given). These files are loaded into a DuckDB, that is exported as
`/out/dataset.db` along with all metadata from MetaCatalog as JSON, and a number of database MACROs for aggregations
along the scale axes. 
For each data integration defined as `integration` (one of `['temporal', 'spatial', 'spatiotemporal']`), the MACRO is
executed and the result is saved to `/out/results/<variable_name>_<entry_id>_<aggregation_scale>_aggs.parquet` containing
aggregations to all statistical moments, quartiles, the sum, Shannon Entropy and a histogram.
The means are further joined into a common `/out/results/mean_<aggregation_scale>_aggs.parquet` as the main result 
outputs. The aggregation is configured via `precision` (temporal) and `resolution` (spatial). The final database
can still be used to execute other aggregations, outside of the context of this tool.

3. The last step can only be run, if the second step was performed successfully. As of now, two finishing report-like
documents are created. First [YData Profiling](https://docs.profiling.ydata.ai/latest/) is run on the 
`/out/results/mean_temporal_aggs.parquet` to create a time-series exploratory data analysis (EDA) report. It is 
availabe in HTML and JSON format.
The second document is a `/out/README.md`, which is created at runtime from the data in the database. Thus, the data 
tables are listed accordingly and license information is extracted and presented as available in the MetaCatalog instance.

### Parameters

| Parameter | Description |
| --- | --- |
| dataset_ids | An array of integers referencing the IDs of the dataset entries in MetaCatalog. |
| reference_area | A valid GeoJSON POLYGON Feature. Areal datasets will be clipped to this area. |
| start_date | The start date of the dataset, if a time dimension applies to the dataset. |
| end_date | The end date of the dataset, if a time dimension applies to the dataset. |
| integration | The mode of operation for integrating all data files associated with each data source into a common DuckDB-based dataset. |
| keep_data_files | If set to `false`, the data files clipped to the spatial and temporal scale will not be kept. |
| precision | The precision for aggregations along the temporal scale of the datasets. |
| resolution | The resolution of the output data. This parameter is only relevant for areal datasets. |
| connection | The connection URI to the MetaCatalog instance. |


## Development and local run

### New database

Either for development or local usage of this container, there is a `docker-compose.yml` file in there.
It starts a PostgreSQL  / PostGIS database, which persists its data into a local `pg_data` folder.
The `loader` service will run the tool, with the local `./in` and `./out` mounted into the tool container and
the database correctly connected.
That means, you can adjust the examples in `./in` and run the container using docker compose:

```
docker compose up -d
```

Obviously, on first run, there won't be a [metacatalog](https://github.com/vforwater/metacatalog) initialized. 
There are examples at `/examples/`, which load different datasets into the same database instance, which can then
be used.
Alternatively, you can populate the database by hand. To create the necessary database structure, you can run the 
loader service but overwrite the default container command with a python console:

```
docker compose run --rm -it loader python
```

Then run:

```python
from metacatalog import api
session = api.connect_database()
api.create_tables(session)
api.populate_defaults(session)
exit()
```

### Existing database

If you want to run the tool on an existing database, change the `METACATALOG_URI` in the `docker-compose.yml`.
Remember, that this will still spin up the database service, thus, for production, you should either remove
the database service from the `docker-compose.yml`, or use docker without docker compose, like:

```
docker build -t vfw_loader .
docker run --rm -it -v /path/to/local/in:/in -v /path/to/local/out:out -v /path/to/local/datafiles:/path/to/local/datafiles -e METACATALOG_URI="postgresql..." vfw_loader 
```

## Structure

This container implements a common file structure inside container to load inputs and outputs of the 
tool. it shares this structures with the [Python template](https://github.com/vforwater/tool_template_python), 
[R template](https://github.com/vforwater/tool_template_r),
[NodeJS template](https://github.com/vforwater/tool_template_node) and [Octave template](https://github.com/vforwater/tool_template_octave), 
but can be mimiced in any container.

Each container needs at least the following structure:

```
/
|- in/
|  |- inputs.json
|- out/
|  |- ...
|- src/
|  |- tool.yml
|  |- run.py
```

* `inputs.json` are parameters. Whichever framework runs the container, this is how parameters are passed.
* `tool.yml` is the tool specification. It contains metadata about the scope of the tool, the number of endpoints (functions) and their parameters
* `run.py` is the tool itself, or a Python script that handles the execution. It has to capture all outputs and either `print` them to console or create files in `/out`

*Does `run.py` take runtime args?*:

Currently, the only way of parameterization is the `inputs.json`. A parameterization via arguments will likely be added in the future.


## How to build the image?

You can build the image from within the root of this repo by
```
docker build -t vfw_loader .
```

The images are also built by a [GitHub Action on each release](https://github.com/VForWaTer/tool_vforwater_loader/pkgs/container/tbr_vforwater_loader).


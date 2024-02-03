# Data Report

This dataset was created using the [Metacatalog Data Loader](https://github/vforwater/tool_vforwater_loader). This tool follows
the Tool Specification for reusable scientific docker tools. 
The loader collects data from various sources and provides a consistent dataset for a specified temporal and spatial extent.
The child datasets are provided in their respective formats, along with a consistent DuckDB instance, which can
easily be accessed from various programming languages. Special emphasis is on data provenance.

## Licenses and owners

This dataset may be a collection of multiple sources. The table below lists all licenses, original data owners and 
citation prompts needed for using this dataset.

${REPORT_LICENSE}

More information about the above licenses can be extracted from the database as:

```SQL
SELECT
    license->short_title AS short,
    license->title AS title,
    license->summary AS summary,
    license->full_text AS body
FROM metadata;
```

## Metadata available

The original data sources are referenced by their respective MetaCatalog entries, which can be used to
identify data layers within this dataset, as well as the original entries in the MetaCatalog instance, that
was used to build this dataset. These are still referenced by their id as shown in the table below:

${REPORT_META}

## Data layers

The data sources described above are extracted and a homogeneous temporal and spatial extent is available in
long-format within this dataset. These sources are referenced as *data layers* and their names are a combination
of data variable and the data source id as described above.
The available data layers in this dataset are listed below:

${REPORT_LAYERS}

### Exporting a layer

You can use DuckDB itself to export these layers into a file. DuckDB is extremely flexible.

#### CLI

Use the DuckDB CLI to extract the data layer `${REPORT_LAYER_EXAMPLE}` in one command
```bash
./duckdb /out/dataset.db -csv -readonly "FROM ${REPORT_LAYER_EXAMPLE};" > ${REPORT_LAYER_EXAMPLE}.csv
./duckdb /out/dataset.db -readonly "COPY ${REPORT_LAYER_EXAMPLE} TO '${REPORT_LAYER_EXAMPLE}.parquet';"
```

#### Python

```python
import duckdb

with duckdb.connect('/out/dataset.db', read_only=True) as db:
    pandas_df = db.sql("from ${REPORT_LAYER_EXAMPLE}").df()
    polars_df = db.sql("from ${REPORT_LAYER_EXAMPLE}").pl()

```

#### R

```R
library('duckdb')

con <- dbConnect(duckdb(), dbdir = "/out/dataset.db", read_only = TRUE)
data.frame <- dbGetQuery("FROM ${REPORT_LAYER_EXAMPLE};")

dbClose(con)
```

## Aggregations

The [Metacatalog Data Loader](https://github/vforwater/tool_vforwater_loader) creates a number of `MACRO`s 
In DuckDB a [MACRO](https://duckdb.org/docs/sql/statements/create_macro.html) can be used similar to a function.
All aggregation MACROs are `TABLE_MACRO`s, which means, the result from these functions can be read like any
other table. Thus, you can use them like the data layers.

The available aggreations are listed below:

${REPORT_AGG}

The column `parameters` lists the names and order of the parameters that have to be passed to the MACRO
`function_name='${REPORT_AGG_EXAMPLE}'`:
Assuming that the MACRO takes a precision, use the `function_name` from the table above like this:

```SQL
SELECT * FROM ${REPORT_AGG_EXAMPLE}('month') ORDER BY time DESC LIMIT 12;
```
This will aggreate the `data_layer` referenced to monthly data and return the latest 12 months from the function.
Note that a usage like this is discouraged, as the MACRO will always perform the aggregation on the full data layer
and limit the result afterwards. The better approach is to `COPY` the result to a parquet and use that for 
subsequent analysis. Additionally, the `/out/processing.log` contains the SQL statement that was used to create the 
MACRO in the first place.

### Sample plot

THe plot below loads the aggregated daily data using the `${REPORT_AGG_EXAMPLE}`:

```text
${REPORT_OVERVIEW_PLOT}
```
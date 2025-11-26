# Pull any base image that includes python3
FROM python:3.12.4

# install the toolbox runner tools
RUN pip install json2args==0.7.0

# Install GDAL which will be used by geopandas
RUN pip install --upgrade pip
RUN apt-get update && apt-get install -y gdal-bin libgdal-dev
# Install numpy first (required for GDAL build)
RUN pip install numpy
# Install GDAL matching the system version
RUN pip install GDAL==$(gdal-config --version | awk -F'[.]' '{print $1"."$2}')

# Install whitebox gis
RUN mkdir /whitebox && \
    cd /whitebox && wget https://www.whiteboxgeo.com/WBT_Linux/WhiteboxTools_linux_amd64.zip && \
    unzip WhiteboxTools_linux_amd64.zip && \
    mv WhiteboxTools_linux_amd64/WBT /whitebox/WBT && \
    rm -rf WhiteboxTools_linux_amd64.zip WhiteboxTools_linux_amd64

# install dependecies for this tool
RUN pip install \
    ipython==8.26.0 \ 
    pandas==2.2.2 \
    geopandas==1.0.1 \
    python-dotenv==1.0.0 \
    xarray[complete]==2024.7.0 \ 
    rioxarray==0.17.0 \
    pyarrow==17.0.0 \
    #ydata-profiling==4.9.0 \
    # linux AArch64 extensions are not available for 0.9.2 -> 0.10.0 is released early Feb. 2024
    #"duckdb>=1.0.0" \
    polars-lts-cpu==1.1.0 \
    geocube==0.6.0 \
    tqdm==4.67.0 \
    metacatalog_api==0.4.4

# Install CDO, might be used to do seltimestep or sellonlatbox and possibly merge
#RUN apt-get install -y gettext=0.21-12 \
    #gnuplot=5.4.4+dfsg1-2 
    # cdo=2.1.1-1 

# create the tool input structure
RUN mkdir /in
COPY ./in /in
RUN mkdir /out
RUN mkdir /src
COPY ./src /src

# copy the citation file - looks funny to make COPY not fail if the file is not there
COPY ./CITATION.cf[f] /src/CITATION.cff

# download a precompiled binary of duckdb
#  first line checks the architecture, and replaces x86_64 with amd64, which is what duckdb uses
# RUN arch=$(uname -m | sed s/x86_64/amd64/) && \     
#     mkdir /duck && \
#     wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-${arch}.zip && \
#     unzip duckdb_cli-linux-${arch}.zip && \
#     rm duckdb_cli-linux-${arch}.zip && \
#     chmod +x ./duckdb && \
#     mv ./duckdb /duck/duckdb

# pre-install the spatial extension into duckdb as it will be used
# RUN /duck/duckdb -c "INSTALL spatial;"

# go to the source directory of this tool
WORKDIR /src
CMD ["python", "run.py"]

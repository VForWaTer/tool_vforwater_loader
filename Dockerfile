# Pull any base image that includes python3
FROM python:3.10.13

# install the toolbox runner tools
RUN pip install json2args==0.6.1

# Install GDAL which will be used by geopandas
RUN pip install --upgrade pip
RUN apt-get update && apt-get install -y gdal-bin libgdal-dev
RUN pip install GDAL==$(gdal-config --version | awk -F'[.]' '{print $1"."$2}')

# Install whitebox gis
RUN mkdir /whitebox && \
    cd /whitebox && wget https://www.whiteboxgeo.com/WBT_Linux/WhiteboxTools_linux_amd64.zip && \
    unzip WhiteboxTools_linux_amd64.zip

# install dependecies for this tool
RUN pip install ipython==8.26.0 \ 
                "pandas<=2.0.0" \
                geopandas==0.14.2 \
                python-dotenv==1.0.0 \
                xarray[complete]==2023.6.0 \ 
                rioxarray==0.15.0 \
                pyarrow==14.0.1 \
                ydata-profiling==4.6.4 \
                # linux AArch64 extensions are not available for 0.9.2 -> 0.10.0 is released early Feb. 2024
                "duckdb>=1.0.0" \
                polars==0.19.19 \
                geocube

# install the needed version for metacatalog
RUN pip install metacatalog==0.9.1

# Install CDO, might be used to do seltimestep or sellonlatbox and possibly merge
RUN apt-get install -y gettext=0.21-12 \
    gnuplot=5.4.4+dfsg1-2 
    # cdo=2.1.1-1 

# create the tool input structure
RUN mkdir /in
COPY ./in /in
RUN mkdir /out
RUN mkdir /src
COPY ./src /src
RUN mv /whitebox/WhiteboxTools_linux_amd64/WBT /src/WBT

# download a precompiled binary of duckdb
#  first line checks the architecture, and replaces x86_64 with amd64, which is what duckdb uses
RUN arch=$(uname -m | sed s/x86_64/amd64/) && \     
    mkdir /duck && \
    wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-${arch}.zip && \
    unzip duckdb_cli-linux-${arch}.zip && \
    rm duckdb_cli-linux-${arch}.zip && \
    chmod +x ./duckdb && \
    mv ./duckdb /duck/duckdb

# pre-install the spatial extension into duckdb as it will be used
RUN /duck/duckdb -c "INSTALL spatial;"

# go to the source directory of this tool
WORKDIR /src
CMD ["python", "run.py"]

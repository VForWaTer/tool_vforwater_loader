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
RUN pip install "pandas<2.2.0"
RUN pip install geopandas==0.14.2
RUN pip install python-dotenv==1.0.0
RUN pip install xarray[complete]==2023.6.0
RUN pip install rioxarray==0.15.0
RUN pip install pyarrow==14.0.1
RUN pip install ydata-profiling==4.6.4
# linux AArch64 extensions are not available for 0.9.2 -> 0.10.0 is released early Feb. 2024
RUN pip install duckdb==0.8.0
RUN pip install polars==0.19.19
RUN pip install geocube

# install the needed version for metacatalog
RUN pip install metacatalog==0.9.0

# Install CDO, might be used to do seltimestep or sellonlatbox and possibly merge
RUN apt-get install -y cdo

# create the tool input structure
RUN mkdir /in
COPY ./in /in
RUN mkdir /out
RUN mkdir /src
COPY ./src /src
RUN mv /whitebox/WBT /src/WBT

# go to the source directory of this tool
WORKDIR /src
RUN chmod +x ./run
CMD ["echo", "Welcome to the tool help page.\nConfig:\n\n", "&&", "cat", "/src/tool.yml"]

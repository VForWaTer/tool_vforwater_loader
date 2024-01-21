# Pull any base image that includes python3
FROM python:3.10.13

# install the toolbox runner tools
RUN pip install json2args==0.6.1

# Install GDAL which will be used by geopandas
RUN pip install --upgrade pip
RUN apt-get update && apt-get install -y gdal-bin libgdal-dev
RUN pip install GDAL==$(gdal-config --version | awk -F'[.]' '{print $1"."$2}')

# install dependecies for this tool
RUN pip install geopandas==0.14.2
RUN pip install python-dotenv==1.0.0
RUN pip install xarray[complete]==2023.6.0
RUN pip install rioxarray==0.15.0
RUN pip install pyarrow==11.0.0
RUN pip install ydata-profiling==4.6.4
RUN pip install duckdb==0.9.2
RUN pip install polars==0.19.19

# install the needed version for metacatalog
RUN pip install metacatalog==0.9.0

# create the tool input structure
RUN mkdir /in
COPY ./in /in
RUN mkdir /out
RUN mkdir /src
COPY ./src /src

WORKDIR /src
CMD ["python", "run.py"]

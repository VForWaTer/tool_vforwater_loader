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
RUN pip install xarray==2023.6.0

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

import os
from datetime import datetime as dt
from dotenv import load_dotenv
from pathlib import Path

from json2args import get_parameter

# parse parameters
kwargs = get_parameter()

# check if a toolname was set in env
toolname = os.environ.get('TOOL_RUN', 'vforwater_loader').lower()

# raise an error if the toolname is not valid
if toolname != 'vforwater_loader':
    raise AttributeError(f"[{dt.now().isocalendar()}] Either no TOOL_RUN environment variable available, or '{toolname}' is not valid.\n")


# extract the dataset ids
dataset_ids = kwargs['dataset_ids']

# extract the optional information
start = kwargs.get('start_date')
end = kwargs.get('end_date')
reference_area = kwargs.get('reference_area')

# check if a connection was given and if it is a valid path
if 'connection' in kwargs:
    if Path(kwargs['connection']).exists():
        load_dotenv(dotenv_path=kwargs['connection'])
    else:
        # it is interpreted as a connection uri
        connection = kwargs['connection']
else:
    load_dotenv()

# check if a connection evironment variable is given
if 'VFW_POSTGRES_URI' in os.environ:
    connection = os.environ['VFW_POSTGRES_URI']
else:
    connection = None


# ----------------------------
# FROM HERE, ONLY DEVELOPMENT

# build the message for now
MSG = f"""
This is the V-FOR-WaTer data loader. Ufortunately, this tool is not yet implemented.

The following information has been submitted to the tool:

START DATE:         {start}
END DATE:           {end}
REFERENCE AREA:     {reference_area is not None}

DATASET IDS:
{', '.join(map(str, dataset_ids))}

DATABASE CONNECTION: {connection is not None}
DATABASE URI:        {connection[:10] + '***' + connection[-6:] if connection is not None else 'N.A.'}

"""

# RUN the tool here and create the output in /out
print(MSG)

# save
with open('/out/result.txt', 'w') as f:
    f.write(MSG)

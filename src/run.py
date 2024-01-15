import os
from datetime import datetime as dt

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


# ----------------------------
# FROM HERE, ONLY DEVELOPMENT

# build the message for now
MSG = f"""
This is the V-FOR-WaTer data loader. Ufortunately, this tool is not yet implemented.

The following information has been submitted to the tool:

START DATE:         {start}
END DATE:           {end}
REFERENCE AREA:     {reference_area is None}

DATASET IDS:
{', '.join(map(str, dataset_ids))}

"""

# RUN the tool here and create the output in /out
print(MSG)

# save
with open('/out/result.txt', 'w') as f:
    f.write(MSG)

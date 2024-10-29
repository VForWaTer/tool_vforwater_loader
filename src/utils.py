from pathlib import Path

import geopandas as gpd
from json2args.logger import logger

from param import load_params

# define a handler for whiteboxgis tools verbose output
def whitebox_log_handler(msg: str):
    # following https://www.whiteboxgeo.com/manual/wbt_book/python_scripting/tool_output.html
    # we can ignore all lines that contain a '%' sign as that is the progress bar
    if '%' in msg or msg.startswith('*'):
        return
    elif 'error' in msg.lower():
        logger.error(f"WhiteboxTools Errored: {msg}")
    else:
        logger.debug(f"WhiteboxTools info: {msg}")


def reference_area_to_file(add_ascii: bool = False) -> str:
    params = load_params()

    # create a geodataframe
    df = gpd.GeoDataFrame.from_features([params.reference_area])

    # save the reference area as a geojson file
    path = Path(params.base_path) / 'reference_area.geojson'
    df.to_file(path, driver='GeoJSON')

    # save the coordinates to a ascii file
    if add_ascii:
        path = Path(params.base_path) / 'reference_area.ascii'
        df.get_coordinates().to_csv(path, sep=' ', header=False, index=False)

    return str(path)
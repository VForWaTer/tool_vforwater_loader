from pathlib import Path
import os
import geopandas as gpd
from json2args.logger import logger

from param import Params

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


def reference_area_to_file(params: Params, add_ascii: bool = False) -> str:
    # params = load_params()

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

def parse_catchment_id(file_path: str) -> str:
    """
    Extract catchment ID from filename.
    Example:
    CAMELS_DE_discharge_sim_DE910910.csv -> DE910910
    """
    fname = os.path.basename(file_path)
    return fname.rsplit("_", 1)[-1].replace(".csv", "").strip()
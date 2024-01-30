from typing import List
from pathlib import Path
import glob
from tqdm import tqdm
import json
import os

from metacatalog import api
from sqlalchemy.orm import Session


def load_hyras_ids(session: Session) -> List[int]:
    entries = api.find_entry(session, title='HYRAS-DE*')
    return [entry.id for entry in entries]


def generate_input_data(hyras_ids: List[int], geojson_path: str = '/tool_init/init/') -> None:
    # find all geojson files
    geojson_files = glob.glob(f'{geojson_path}/*.geojson')
    out_path = Path(geojson_path).parent

    # create a folder for each catchment
    for geojson_file in tqdm(geojson_files):
        # get the id of the catchment
        catchment_id = Path(geojson_file).stem

        # create a folder for the catchment
        catchment_base_path = out_path / catchment_id
        catchment_base_path.mkdir(parents=True, exist_ok=True)

        # create a in and out folder
        in_folder = catchment_base_path / 'in'
        in_folder.mkdir(parents=True, exist_ok=True)
        out_folder = catchment_base_path / 'out'
        out_folder.mkdir(parents=True, exist_ok=True)

        # read in the geojson as json
        with open(geojson_file, 'r') as f:
            geojson = json.load(f)
        
        # get the feature
        feature = geojson['features'][0]

        # build the inputs.json template
        template = dict(
            vforwater_loader=dict(
                parameters=dict(
                    dataset_ids=hyras_ids,
                    start_date=f"{os.getenv('START_YEAR', '2000')}-01-01T12:00:00+01",
                    end_date=f"{os.getenv('END_YEAR', '2010')}-12-31T12:00:00+01",
                    reference_area=feature
                )
            )
        )

        # write the template to the in folder
        with open(in_folder / 'inputs.json', 'w') as f:
            json.dump(template, f, indent=4)

if __name__ == '__main__':
    print('Generating some example tool input parameter files for CAMELS-DE catchments using HYRAS-DE.')
    # TODO: here we could accept some args and download the camels catchments in the first place
    # create a session to the database
    session = api.connect_database()

    # get the ids of the hyras entries
    hyras_ids = load_hyras_ids(session)

    # generate the input data
    generate_input_data(hyras_ids)

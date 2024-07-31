import subprocess
import time

import pandas as pd
from ydata_profiling import ProfileReport

from param import load_params
from json2args.logger import logger


def generate_profile_report(file_name: str) -> None:
    # get the params
    params = load_params()

    # open the file - for now hardcoded
    source_path = params.result_path / 'mean_temporal_aggs.parquet'
    target_path = params.result_path / 'mean_temporal_aggs_profile.html'
    json_path = params.result_path / 'mean_temporal_aggs_profile.json'

    # check if the means have been compiled
    if not source_path.exists():
        logger.warning(f"Cannot generate a EDA report for '{source_path}' as it does not exist. This is expected behavior if integration is not 'all' or 'temporal'.")
        return
    
    # load the data
    df = pd.read_parquet(source_path).set_index('time')

    # create the report
    t1 = time.time()
    report = ProfileReport(df, tsmode=True, title='MetaCatalog dataset EDA Report', explorative=True)
    report.report
    t2 = time.time()

    # save the report
    with open(target_path, 'w') as f:
        f.write(report.to_html())
    
    # save the json
    with open(json_path, 'w') as f:
        f.write(report.to_json())

    # log 
    logger.info(f"Generated EDA report for '{source_path}' in {t2-t1:.2f} seconds. HTML report in {target_path} and JSON report in {json_path}.")


def generate_readme() -> None:
    # get the params
    params = load_params()

    # check if the database path exists
    if not params.database_path.exists():
        logger.warning(f"Cannot generate a README for Dataset '{params.database_path}' as it does not exist. This is expected behavior if integration is 'none'.")
        return
    
    # create the cmd
    cmd = ['bash', 'src/markdown_report.sh']

    # start the timer
    t1 = time.time()
    subprocess.run(cmd)
    t2 = time.time()

    # log the result
    logger.info(' '.join(cmd))
    logger.info(f"Generated README in {t2-t1:.2f} seconds.")


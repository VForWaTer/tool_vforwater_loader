import os
from pathlib import Path
import subprocess

from metacatalog import api
from sqlalchemy.exc import ProgrammingError


def install_pg():
    # get a database session
    session = api.connect_database()

    # set the init script flag to True, this will request the init script to be run
    run_init_script = True

    #check if the database exists
    try:
        # try to find at least one entry
        entries = api.find_entry(session, title='*')
        if len(entries) > 0:
            # there are entries, so we assume the init script was alread run
            run_init_script = False
    except ProgrammingError as e:
        session.rollback()
        
        if 'relation "entries" does not exist' in str(e):
            # create the database
            api.create_tables(session)
            api.populate_defaults(session)
        else:
            raise e
    
    # finally run the init script and indicate if this is the first run of the tool
    # the user can set the environment variable FORCE_INIT to True to run the scirpt anyway
    init_script(run_init_script)


def init_script(first_install: bool = False):
    # the first_install may be false, but the environment variable FORCE_INIT may be set to True
    # in that case we run the init script anyway
    do_run = os.environ.get('FORCE_INIT', 'False').lower() == 'true' or first_install

    # no init needed, so do not run
    if not do_run:
        return 
    
    # database is installed now.
    # now check if the path /pg_init/init.sh exists
    # that will contain details on how to further upload data to the database
    if Path('/tool_init/init.sh').exists():
        subprocess.run(['bash', '/tool_init/init.sh'])

if __name__ == '__main__':
    install_pg()

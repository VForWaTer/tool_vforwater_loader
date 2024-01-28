from pathlib import Path
import subprocess

from metacatalog import api
from sqlalchemy.exc import ProgrammingError


def install_pg():
    session = api.connect_database()

    #check if the database exists
    try:
        # try to find at least one entry
        entries = api.find_entry(session, title='*')
        if len(entries) > 0:
            return
    except ProgrammingError as e:
        session.rollback()
        
        if 'relation "entries" does not exist' in str(e):
            # create the database
            api.create_tables(session)
            api.populate_defaults(session)
        else:
            raise e

    # database is installed now.
    # now check if the path /pg_init/init.sh exists
    # that will contain details on how to further upload data to the database
    if Path('/tool_init/init.sh').exists():
        subprocess.run(['bash', '/tool_init/init.sh'])

if __name__ == '__main__':
    install_pg()

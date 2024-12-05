import os
from utils import app
from ivsdb import IVSdata, models


def analyzed(session):
    user = os.getenv('SUDO_USER', os.getenv('USER'))

    # Get database url
    db_url = app.load_control_file(name=app.ControlFiles.Database)[-1]['Credentials'][app.args.db]
    with IVSdata(db_url) as dbase:
        record = models.AnalyzedSession(session, user)
        dbase.add(record)
        dbase.commit()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Logger for ADAP software.' )
    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('session')

    args = app.init(parser.parse_args())

    analyzed(args.session)


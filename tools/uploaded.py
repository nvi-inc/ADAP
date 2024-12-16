from utils import app
from ivsdb import IVSdata, models


def uploaded(dbase, filename):

    record = models.UploadedFile()
    record.name, record.user, record.application = filename, user, application
    record.updated = uploaded_time

    dbase.add(record)

    dbase.commit()

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Logger for ADAP software.' )
    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('filename')

    args = app.init(parser.parse_args())

    db_url = app.load_control_file(name=app.ControlFiles.Database)[-1]['Credentials'][app.args.db]

    with IVSdata(db_url) as dbase:
        uploaded(dbase, args.filename)


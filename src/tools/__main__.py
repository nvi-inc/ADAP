import os
import sys
from datetime import datetime, timedelta

from tabulate import tabulate
from sqlalchemy import and_

from utils import app, readDICT
from tools import record_submitted_files
from ivsdb import IVSdata, models


# Extract successful information from cddis output message and update database
def submitted():
    user, application = os.getenv('SUDO_USER', os.getenv('USER')), 'cddis'
    text = sys.stdin.read()
    if files := [models.UploadedFile(line.split()[-1].strip(), user, application, 'ok') for line in text.splitlines()
                 if line.startswith('Successful upload')]:
        record_submitted_files(files)
    print(text)


# List files that have been uploaded to cddis
def uploaded(files, days):
    db_url = app.load_control_file(name=app.ControlFiles.Database)[-1]['Credentials'][app.args.db]

    headers = ['Name', 'User', 'Application', 'Uploaded', 'Status']
    table = []
    with IVSdata(db_url) as dbase:
        for name in files:
            if not (records := dbase.orm_ses.query(models.UploadedFile).filter(models.UploadedFile.name == name)
                    .order_by(models.UploadedFile.updated.asc()).all()):
                records = dbase.orm_ses.query(models.UploadedFile).filter(models.UploadedFile.name.like(f'{name}%')) \
                    .order_by(models.UploadedFile.updated.asc()).all()
            if not records:
                since = datetime.now() - timedelta(days=days)
                records = dbase.orm_ses.query(models.UploadedFile).filter(
                    and_(models.UploadedFile.user.like(f'{name}%'), models.UploadedFile.updated > since)) \
                    .order_by(models.UploadedFile.updated.asc()).all()
            table.extend([[rec.name, rec.user, rec.application, rec.updated, rec.status] for rec in records])
    print(tabulate(table, headers, tablefmt="fancy_grid"))


# Test a toml file to make sure it is well formatted
def toml(path):
    print(path, os.path.exists(path))
    print(readDICT(path, exit_on_error=True))
    for key in readDICT(path).keys():
        print(key)


# Return path of session of vgosDB folder
def wd(name):
    try:
        # Get database url from hidden file
        db_url, db_tunnel = app.get_dbase_info()
        folder = name
        # Retrieve session information using database
        with IVSdata(db_url, db_tunnel) as dbase:
            if ses := dbase.get_session(name):
                folder = str(ses.folder)
            elif (ses_id := dbase.get_db_session_code(name)) and (ses := dbase.get_session(ses_id)):
                folder = str(ses.db_folder)
    except Exception:
        pass
    sys.stdout.write(folder)


# Return the information of a specific session
def session(name):
    url, tunnel = app.get_dbase_info()
    with IVSdata(url, tunnel) as dbase:
        if session := dbase.get_session(name.lower()):
            print(session)
        elif (ses_id := dbase.get_db_session_code(name.upper())) and (session := dbase.get_session(ses_id)):
            print(session)
        else:
            print(f'{name} does not exists!')


def next_sessions(intensive):
    url, tunnel = app.get_dbase_info()
    with IVSdata(url, tunnel) as dbase:
        start = datetime.utcnow() - timedelta(days=1)
        end = datetime.utcnow() + timedelta(days=7)
        master = ['intensive'] if intensive else ['standard']
        for ses_id in dbase.get_sessions(start, end, master):
            if ses := dbase.get_session(ses_id):
                print(ses)


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser(description='Tools ')
    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-days', required=False, type=int, default=7)
    parser.add_argument('-toml', required=False)
    parser.add_argument('-wd', required=False)
    parser.add_argument('-submitted', action='store_true')
    parser.add_argument('-uploaded', action='store_true')
    parser.add_argument('-next', help='list upcoming sessions', action='store_true')
    parser.add_argument('-i', '--intensive', help='list upcoming sessions', action='store_true')
    parser.add_argument('params', help='uploaded filenames', nargs='*')

    args = app.init(parser.parse_args())

    if args.toml:
        toml(args.toml)
    elif args.uploaded:
        uploaded(args.params, args.days)
    elif args.submitted:
        submitted()
    elif args.wd:
        wd(args.wd)
    elif args.next:
        next_sessions(args.intensive)
    else:
        session(args.params[0])

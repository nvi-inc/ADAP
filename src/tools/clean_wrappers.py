import os
from pathlib import Path
from collections import defaultdict

from utils import app
from vgosdb import VGOSdb


def get_session(dbase, file):
    if (ses_id := dbase.get_db_session_code(file.name)) and (session := dbase.get_session(ses_id)):
        return session if file.name == session.db_name else None
    return None


def get_renamed_session(dbase, file):
    if (ses_id := dbase.get_db_session_code(file.name)) and (session := dbase.get_session(ses_id)):
        return session.code if file.name != session.db_name else None
    return None


def get_vgosdbs(dbase, year):
    print(f'Processing {year} vgosDB')
    folder = Path(app.VLBIfolders.vgosdb, year)
    renamed = defaultdict(list)
    for file in folder.iterdir():
        if file.is_dir() and (ses_id := get_renamed_session(dbase, file)):
            renamed[ses_id].append(file)
    for file in folder.iterdir():
        if file.is_dir() and (session := get_session(dbase, file)) and renamed.get(session.code, None):
            fix_vgosdb(session)


def fix_vgosdb(session):
    vgosdb = VGOSdb(session.db_folder)
    vgosdb.get_wrappers()
    for wrapper in vgosdb.wrappers:
        if wrapper.version < 'V002':
            break
        if wrapper.time_tag < vgosdb.create_time:
            print(f'{vgosdb.name} {vgosdb.create_time} {wrapper.name:30} {wrapper.time_tag}')
            os.remove(Path(session.db_folder, wrapper.name))


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Logger for ADAP software.' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-y', '--year', help='year to process', required=False)
    parser.add_argument('vgosdb', help='vgosdb name', default='', nargs='?')

    args = app.init(parser.parse_args())

    dbase = app.get_dbase()
    if args.year:
        for year in [str(i) for i in range(1979, 2025)] if args.year == 'all' else [args.year]:
            get_vgosdbs(dbase, year)
    elif (ses_id := dbase.get_db_session_code(args.vgosdb)) and (session := dbase.get_session(ses_id)):
        fix_vgosdb(session.db_folder)


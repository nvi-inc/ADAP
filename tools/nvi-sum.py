from datetime import date, datetime, timedelta
from collections import defaultdict

from utils import app
from ivsdb import IVSdata, models


def make_summary(stations):
    today = date.today()
    end = today - timedelta(days=(today.weekday() + 1))
    start = end - timedelta(days=6)

    print(f'Summary Report for period: {start}-{end}\n')

    stations = list(map(str.lower, stations))

    vgos = defaultdict(list)
    legacy = defaultdict(list)

    dbase = app.get_dbase()
    for ses_id in dbase.get_sessions(start, end, ['standard', 'intensive']):
        if session := dbase.get_session(ses_id):
            for sta_id in stations:
                if sta_id in session.included:
                    sessions = vgos if session.name.startswith('vgos') else legacy
                    sessions[sta_id].append(ses_id.upper())

    for sta_id in stations:
        if sta_id in vgos:
            print(f'{sta_id.capitalize()} (VGOS): {", ".join(vgos[sta_id])}')
        if sta_id in legacy:
            print(f'{sta_id.capitalize()}: {", ".join(legacy[sta_id])}')
        if sta_id not in vgos and sta_id not in legacy:
            print(f'{sta_id.capitalize()}:')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Logger for ADAP software.' )
    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('stations', nargs='+')

    args = app.init(parser.parse_args())

    make_summary(args.stations)

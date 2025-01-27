from utils import app
from vgosdb import VGOSdb

def read_snranal(name, path):
    with open(path) as f:
        for line in f:
            if line.strip() == 'SEFD':
                for line in f:
                    if line.startswith(name):
                        return line.split()[1:]
                    if not line.strip():
                        return None

def get_sefds(sta_id, begin, end):
    dbase = app.get_dbase()
    name = dbase.get_station(sta_id.capitalize()).name
    for ses_id in dbase.get_sessions(begin, end, ['standard']):
        if (session := dbase.get_session(ses_id)) and (sta_id in session.included):
            if name in VGOSdb(session.db_folder).station_list:
                if (snranal := session.file_path('snranal')).exists() and (values := read_snranal(name, snranal)):
                    print(f"{session.code},{session.start.strftime('%Y-%m-%d')},{values[0]},{values[2]}")

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Network Performance', prog='vcc-ns', add_help=False)
    parser.add_argument('-c', '--config', help='config file', required=False)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('action', choices=['sefd', 'missed'], type=str.lower)

    action = parser.parse_known_args()[0].action
    if action in ('sefd'):
        parser.add_argument('station', help='station code', type=str.lower)
        parser.add_argument('begin', help='beginning date', type=str)
        parser.add_argument('end', help='ending date', type=str)

        args = app.init(parser.parse_args())
        get_sefds(args.station, args.begin, args.end)
    else:
        print('not valid action')

if __name__ == '__main__':
    import sys

    sys.exit(main())

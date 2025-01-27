from pathlib import Path
import json
from pathlib import Path
from collections import defaultdict, Counter

from utils import app
from performance import AnnualPerformance, SessionPerformance
from schedule import get_schedule


class NameStats:
    def __init__(self):
        self.nbr = 0
        self.cancel = 0
        self.not_obs = []
        self.one = []
        self.obs = []
        self.corr = 0

    def processed(self, session):
        self.nbr += 1
        if session.db_folder.exists():
            self.corr += 1
            return True
        if len(session.included) < 2:
            self.cancel += 1
            return False
        if (nbr := len([file for file in session.folder.glob('*.log')])) < 1:
            self.not_obs.append(session.code)
        elif nbr < 2:
            self.one.append(session.code)
        else:
            self.obs.append(session.code)
        return False

    def summary(self):
        return f"{self.nbr:4d} {self.cancel:4d} {len(self.not_obs):4} {len(self.one):4} {len(self.obs):4}"


def intensive(dbase, year):

    nbr_session = nbr_cancelled = 0

    names = defaultdict(NameStats)

    stats = {}
    not_corr, observed, not_observed, only_one = [], [], [], []
    for (ses_id, start) in dbase.get_sessions_from_year(year, ['intensive']):
        session = dbase.get_session(ses_id)
        nbr_session += 1
        if not names[session.name.upper()].processed(session):
            continue

        schedule = get_schedule(session)
        if session.removed:
            schedule.remove_stations([dbase.get_station(sta).name for sta in session.removed])
        for code in schedule.stations['codes'].keys():
            if code not in stats:
                stats[code] = dict(sessions=0, scheduled=0, analyzed=0, used=0, cancelled=0)

            stats[code]['scheduled'] += schedule.stations['codes'][code]['scheduled_obs']
            stats[code]['sessions'] += 1

        stations = defaultdict(list)
        perf = SessionPerformance(session, {})
        for obs in perf.observations:
            stations[obs['fr']].append(obs)
            stations[obs['to']].append(obs)

        for code, obs in stations.items():
            sta_stats = stats[code]
            sta_stats['analyzed'] += len([True for o in obs if o['analyzed']])
            sta_stats['used'] += len([True for o in obs if o['used']])

    for code, info in stats.items():
        if info['scheduled']:
            print(f"{dbase.get_station(code).name:<10s} {code} {info['sessions']:4d} {info['scheduled']:6d} "
                  f"{info['used']:6d} {info['used'] / info['scheduled'] * 100:5.1f}")

    for name, info in names.items():
        print(f"{name:<20s} {info.summary()}")
    for name, info in names.items():
        print(name, info.not_obs)
        print(name, info.one)
        print(name, info.obs)


def build(dbase, annual, year):
    index = 0
    for (ses_id, start) in dbase.get_sessions_from_year(year, ['standard']):
        if (cmt := Path(folder, f'{ses_id}.cmt')).exists() \
                and (ses := dbase.get_session(ses_id)).db_folder.exists():
            if True:  # ses_id.lower() == 'aua048':  #True:  # if not ses.corr_db_code.startswith(('V', 'v')):
                index += 1
                print(index, ses_id, end=' ')
                if annual.process(ses, cmt):
                    annual.save()
                    print('saved')
                else:
                    print('not processed')



if __name__ == '__main__':
    import argparse
    from ivsdb import IVSdata, models

    parser = argparse.ArgumentParser(description='Extract SEFDs from logs')
    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-s', '--summary', action='store_true')
    parser.add_argument('-i', '--intensive', action='store_true')
    parser.add_argument('-r', '--report', action='store_true')
    parser.add_argument('year')

    app.init(parser.parse_args())

    folder = f'/sgpvlbi/projects/stats/{app.args.year}'
    stat = Path(f'/sgpvlbi/projects/stats/{app.args.year}.stats')
    url, tunnel = app.get_dbase_info()
    index = 0
    with IVSdata(url, tunnel) as dbase:
        if app.args.intensive:
            intensive(dbase, app.args.year)
        else:
            annual = AnnualPerformance(stat)
            if app.args.summary:
                annual.summary(dbase)
            else:
                build(dbase, annual, app.args.year)

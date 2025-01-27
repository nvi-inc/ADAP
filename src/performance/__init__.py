import json
from datetime import timedelta
from collections import defaultdict, Counter
from operator import itemgetter
from pathlib import Path
import re

import toml

from copy import deepcopy
from schedule import get_schedule
from vgosdb import VGOSdb

src_src = toml.load('/sgpvlbi/projects/stats/source.toml')


class NetworkPerformance:
    def __int__(self, stats):
        self.stations = defaultdict(StationPerformance)
        self.scheduled = self.scans = self.used = self.warm = 0
        self.dropped = defaultdict(int)
        self.problems = dict(ant=0, clock=0, ops=0, power=0, rack=0, receiver=0, recorder=0, rfi=0, shipping=0)

    def process(self, session, comments):
        pass


keys = ('ant', 'clock', 'ops', 'power', 'rack', 'receiver', 'recorder', 'rfi', 'shipping', 'unknown',
        'misc', 'software')
alias = {'oper': 'ops'}
vgos = ('Gs', 'Hb', 'Is', 'K2', 'Ke', '')

class StationPerformance:
    def __init__(self):
        self.nbr_sessions = 0
        self.scheduled = self.recoverable = self.scans = self.used = self.warm = 0
        self.dropped = defaultdict(int)
        self.lost = 0
        self.problems = {key: 0 for key in keys}

    def __str__(self):
        return f'{self.nbr_sessions:5d} {self.lost:5.1f} {self.scheduled:5d} ' \
               f'{self.recoverable:5d} {self.used:5d}'

    def add(self, ses_id, sta_id, stat):
        self.nbr_sessions += 1

        self.scheduled += stat['scheduled']
        self.recoverable += stat['recoverable']
        self.used += stat['used']
        if nscans := stat.get('scans', 0):
            missed = stat.get('missed', {})
            problems = {key: missed.get(key, 0) / nscans for key in keys}
            for key, alt in alias.items():
                problems[alt] += missed.get(key, 0) / nscans

            ok = 1.0 - min(sum([n for n in problems.values()]), nscans)
            if (nchannels := stat['channels']) == 64:
                dropped = sum([2 if '/' in ch else 1 for ch in stat.get('dropped', [])])
            else:
                dropped = sum([2 if '/' in ch else 1 for ch in stat.get('dropped', []) if not ch.endswith('Y')])

            if dropped:
                problems['rfi'] += ok * min(dropped, 3) / nchannels
            if stat.get('warm', False):
                problems['receiver'] += ok * 2 / 3

            for key in self.problems.keys():
                self.problems[key] += problems.get(key, 0)
            self.lost += min(1, sum([n for n in problems.values()]))


def get_sta():
    print('GetSta')
    return StationPerformance()


class AnnualPerformance:
    def __init__(self, path):

        self.path = path
        self.stations = defaultdict(StationPerformance)
        self.stats = json.load(open(self.path)) if self.path.exists() else {}

    def process(self, session, comments):
        if self.stats.get(session.code):
            return False

        ses = SessionPerformance(session, comments)
        self.stats[session.code] = ses.statistics()
        return True

    def save(self):
        with open(self.path, 'w+') as f:
            json.dump(self.stats, f)

    def _summary(self, dbase):
        nbr_sessions = scheduled_stations = proposed_stations = 0
        scheduled_obs = correlated_obs = used_obs = 0
        for ses_id, info in self.stats.items():
            nbr_sessions += 1
            session = dbase.get_session(ses_id)
            proposed_stations += len(session.stations)
            scheduled_stations += len(info.keys())
            scheduled = correlated = used = 0
            for sta_id, stat in info.items():
                sta_stat = self.stations[sta_id]
                sta_stat.scheduled += stat['scheduled']
                scheduled += stat['scheduled']
                correlated += stat['recoverable']
                sta_stat.correlated += stat['recoverable']
                used += stat['used']
            scheduled_obs += int(scheduled / 2)
            correlated_obs += int(correlated / 2)
            used_obs += int(used / 2)
            percentage_correlated = int(correlated_obs/scheduled_obs*100)
            percentage_used = int(used_obs / scheduled_obs * 100)
        print(nbr_sessions, proposed_stations, scheduled_stations, scheduled_obs,
              percentage_correlated, percentage_used)

    def summary(self, dbase):
        nbr_sessions = scheduled_stations = proposed_stations = 0
        scheduled_obs = correlated_obs = used_obs = 0
        for ses_id, info in self.stats.items():
            nbr_sessions += 1
            session = dbase.get_session(ses_id)
            proposed_stations += len(session.stations)
            scheduled_stations += len(info.keys())
            scheduled = correlated = used = 0
            for sta_id, stat in info.items():
                self.stations[sta_id].add(ses_id, sta_id, stat)
                scheduled += stat['scheduled']
                correlated += stat['recoverable']
                used += stat['used']
            scheduled_obs += int(scheduled / 2)
            correlated_obs += int(correlated / 2)
            used_obs += int(used / 2)
            percentage_correlated = int(correlated_obs/scheduled_obs*100)
            percentage_used = int(used_obs / scheduled_obs * 100)

        problems = {key: 0 for key in keys}
        for sta_id, info in self.stations.items():
            print(f'{sta_id} {info}')
            for key, val in info.problems.items():
                problems[key] += val

        print(nbr_sessions, proposed_stations, scheduled_stations, scheduled_obs,
              percentage_correlated, percentage_used)
        for key, val in problems.items():
            print(f'{key:10s} {val / scheduled_stations * 100:7.2f}')


class SessionPerformance:

    def __init__(self, session, comments):
        self.has_problems = False
        self.schedule = get_schedule(session)
        self.stations = sorted(self.schedule.stations['codes'].keys())
        self.names = {code: self.schedule.stations['codes'][code]['name'] for code in self.stations}
        self.codes = {self.schedule.stations['codes'][code]['name']: code for code in self.stations}
        self.comments = comments
        self.observations, self.scans, self.sources, self.baselines, self.missed = self.get_observations(session)
        self.clean_miss_identified()
        self.nbr_channels, self.dropped = self.get_dropped_channels(session)

    def get_missed(self, code, scheduled):
        missed = {}
        for problem in self.comments.get('problems', []):
            cause = problem['code']
            if 'scans' in problem:
                for scans in problem['scans']:
                    if scans == 'all':
                        return {scan: cause for scan in scheduled[code]}
                    limits = scans.split(',')
                    fr, to = limits[0], limits[-1]
                    for scan in scheduled[code]:
                        if fr <= scan <= to:
                            missed[scan] = cause

        return missed

    def _get_observations(self, session):

        scheduled = {code: list(self.schedule.stations['codes'][code]['scans'].keys()) for code in self.stations}
        skd_src = [name for name in self.schedule.sources]
        missed = {code: self.get_missed(code, scheduled) for code in self.stations}

        # Count observations for stations
        observations = []
        keys = defaultdict(list)
        scans = defaultdict(list)
        sources = defaultdict(list)
        baselines = defaultdict(list)
        for obs in self.schedule.obs_list:
            scan, fr, to = obs['scan'], obs['fr'], obs['to']
            name, source, start = scan['name'], scan['source'], scan['start']
            duration = min(scan['station_codes'][fr]['duration'], scan['station_codes'][to]['duration'])
            end = start + timedelta(seconds=duration)
            info = {'fr': fr, 'to': to, 'start': start, 'end': end, 'source': source, 'scan': name,
                    'missed': {fr: missed[fr].get(name, None), to: missed[to].get(name, None)},
                    'correlated': False, 'recoverable': False, 'analyzed': False, 'used': False}
            observations.append(info)
            scans[name].append(info)
            sources[source].append(info)
            baselines[f'{fr}-{to}'] = baselines[f'{to}-{fr}'] = info
            keys[f'{self.names[fr]}-{self.names[to]}'].append(info)
            keys[f'{self.names[to]}-{self.names[fr]}'].append(info)

        vdb = VGOSdb(session.db_folder)

        # Compile stats
        good_qc = b'56789'
        not_in_src = []
        for index, bl, src, utc, qc_x, qc_s, fc_x, fc_s, flg in vdb.get_all_obs():
            if src not in skd_src:
                src = src_src.get(src, src)
            key = f'{bl[0]}-{bl[1]}'

            for obs in keys[key]:
                if obs['start'] <= utc <= obs['end']:
                    obs['correlated'], obs['analyzed'] = True, flg <= 1 and qc_s in good_qc and qc_x in good_qc
                    obs['used'] = obs['analyzed'] and flg == 0
                    break
            else:
                print('Not found', session.code, key, bl, src, utc, qc_x, qc_s, fc_x, fc_s, flg)
                self.has_problems = True
                not_in_src.append(src)

        if self.has_problems:
            print('Not in skd', list(set(not_in_src)))
            print('Not in vgosDB', [name for name in skd_src if name not in vdb.sources])

        return observations, scans, sources, baselines, missed

    def get_observations(self, session):

        scheduled = {code: list(self.schedule.stations['codes'][code]['scans'].keys()) for code in self.stations}
        skd_src = [name for name in self.schedule.sources]
        missed = {code: self.get_missed(code, scheduled) for code in self.stations}

        # Count observations for stations
        observations = []
        keys = defaultdict(list)
        scans = defaultdict(list)
        sources = defaultdict(list)
        baselines = defaultdict(list)
        for obs in self.schedule.obs_list:
            scan, fr, to = obs['scan'], obs['fr'], obs['to']
            name, source, start = scan['name'], scan['source'], scan['start']
            duration = min(scan['station_codes'][fr]['duration'], scan['station_codes'][to]['duration'])
            end = start + timedelta(seconds=duration)
            info = {'fr': fr, 'to': to, 'start': start, 'end': end, 'source': source, 'scan': name,
                    'missed': {fr: missed[fr].get(name, None), to: missed[to].get(name, None)},
                    'correlated': False, 'recoverable': False, 'analyzed': False, 'used': False}
            observations.append(info)
            scans[name].append(info)
            sources[source].append(info)
            baselines[f'{fr}-{to}'] = baselines[f'{to}-{fr}'] = info
            keys[f'{self.names[fr]}-{self.names[to]}'].append(info)
            keys[f'{self.names[to]}-{self.names[fr]}'].append(info)

        vdb = VGOSdb(session.db_folder)

        # Compile stats
        good_qc = b'56789'
        not_in_src = []
        for index, bl, src, utc, qc_x, qc_s, fc_x, fc_s, flg in vdb.get_all_obs():
            if src not in skd_src:
                src = src_src.get(src, src)
            key = f'{bl[0]}-{bl[1]}'

            for obs in keys[key]:
                if obs['start'] <= utc <= obs['end']:
                    obs['correlated'], obs['analyzed'] = True, flg <= 1 and qc_s in good_qc and qc_x in good_qc
                    obs['used'] = obs['analyzed'] and flg == 0
                    break
            else:
                #print('Not found', session.code, key, bl, src, utc, qc_x, qc_s, fc_x, fc_s, flg)
                self.has_problems = True
                not_in_src.append(src)

        #if self.has_problems:
        #    print('Not in skd', list(set(not_in_src)))
        #    print('Not in vgosDB', [name for name in skd_src if name not in vdb.sources])

        return observations, scans, sources, baselines, missed

    def clean_miss_identified(self):
        for obs in self.observations:
            if any(obs['missed'].values()) and obs['used']:
                for key, cause in obs['missed'].items():
                    if cause:  # and obs['scan'] in self.missed[key]:
                        self.missed[key].pop(obs['scan'], None)
                obs['missed'] = {key: None for key in obs['missed']}

    def clean_scans_(self):

        index = 0
        for name, obs_list in self.scans.items():
            rejected = []
            observed = []
            for obs in obs_list:
                observed.extend(list(obs['missed'].keys()))
                if not obs['recoverable'] and not any(obs['missed'].values()):
                    rejected.extend(list(obs['missed'].keys()))
            if rejected:
                index += 1
                print(index, name, len(obs), len(rejected), Counter(rejected), Counter(observed))
                print(index, name, [obs['missed'] for obs in obs_list if not obs['recoverable']])

    def statistics(self):
        stations = defaultdict(list)
        stats = {}
        for obs in self.observations:
            stations[obs['fr']].append(obs)
            stations[obs['to']].append(obs)

        for code, obs in stations.items():
            stations[code] = sorted(obs, key=itemgetter('start'))
            stats[code] = {'scans': self.comments.get('nscans', 0),
                           'scheduled': len(stations[code]),
                           'possible': len([True for obs in stations[code] for key, cause in obs['missed'].items()
                                            if key != code and not cause]),
                           'recoverable': len([True for obs in stations[code] if obs['analyzed']]),
                           'used': len([True for obs in stations[code] if obs['used']]),
                           'channels': self.nbr_channels,
                           'dropped': self.dropped.get(code, []),
                           'missed': dict(Counter([cause for cause in self.missed.get(code, {}).values()])),
                           'warm': self.comments.get('warm', 'no').lower() == 'yes'
                           }
        return stats

    def summary(self):
        miss_identified = defaultdict(list)
        for code, observations in self.stations.items():
            for obs in observations:
                if any(obs['missed'].values()) and obs['used']:
                    print(code, obs['scan'], obs['start'], obs['fr'], obs['to'], obs['missed'])
                    miss_identified[code].append(deepcopy(obs))
                    obs['missed'] = {key: None for key in obs['missed']}

        print('Not correlated')
        index = 0
        for code, observations in self.stations.items():
            for obs in observations:
                if not any(obs['missed'].values()) and not obs['correlated']:
                    index += 1
                    print(index, code, obs['scan'], obs['start'], obs['fr'], obs['to'], obs['missed'])
                    print(index, [(o['missed'], o['used']) for o in self.scans[obs['scan']]])
        return
        print('Used')
        for code, observations in miss_identified.items():
            for obs in observations:
                print(code, obs['scan'], obs['start'], obs['fr'], obs['to'], obs['missed'])

    def get_dropped_channels(self, session):
        dropped = {}
        file = None
        for file in Path(session.db_folder, 'History').glob('*V000_kMk4.hist'):
            with open(file, errors='ignore') as f:
                txt = f.read()
        if not file:
            return 16, dropped
        nchannels = 0
        if channels := re.search(r'\+CHANNELS(.*)\+DROP_CHANNELS', txt, re.DOTALL):
            for line in channels.group(1).splitlines():
                if line.startswith(('X', 'S')):
                    nchannels += 1
        if found := re.search(r'\+DROP_CHANNELS(.*)\+MANUAL', txt, re.DOTALL):
            for line in found.group(1).splitlines():
                if line.strip():
                    sta, *lst = line.split()
                    if len(sta) == 2:
                        dropped[sta] = [ch.replace(',', '') for ch in lst if ch.startswith(('X', 'S'))]
        return nchannels if nchannels == 64 else 16, dropped

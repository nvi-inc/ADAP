import re
from pathlib import Path
from collections import defaultdict

from utils import app


def addSNRratio(ratio, sum, n):
    N = ratio['n']
    sum = ratio['sum'] * N + sum * n
    N += n
    ratio['n'] = N
    if N > 0:
        ratio['sum'] = sum / N

def get_channel(code, frequencies, channels):
    index = '{:d}'.format(frequencies.index(code[0:1]) + 1)
    if index not in channels:
        bbc, _ = zip(*channels.values())
        print('WARNING! {} not in list of channels [{}]'.format(index, ','.join(list(bbc))))
        return 'N/A'

    name = channels[index][0]
    if '+' in code:
        name = name[0:3] + 'L'
    elif '-' in code:
        name = name[0:3] + 'U'
    return name

def process_summary(line, ses):
    if '5-9' in line and isfloat(line.split()[1].replace('%', '')):
        ses.skd.data['corr_loss'] = 1.0 - tofloat(line.split()[1].replace('%', '')) / 100.0

def process_notes(line, ses):
    if ':' not in line:
        return

    info = re.sub(r'\W+', ' ', line.split(':')[0].strip()).split()
    if len(info) == 3 and len(info[1]) == 2 and len(info[2]) == 1:
        data = ses.skd.data
        if info[0] not in data['station_names']:
            if info[1] not in data['station_codes']:
                data['station_codes'][info[1]] = SKD.init_sta(code=info[1], name=info[0], key=info[2])
            data['station_names'][info[0]] = data['station_codes'][info[1]]
        data['station_keys'][info[2]] = data['station_names'][info[0]]

def process_dropped_channels(line, ses):
    line = line.strip()
    if line:
        info = re.sub(r'\W+', ' ', line).split()
        if len(info[0]) == 2:
            sta = info[0]
            dropped = info[1:]
        else:
            sta = info[1]
            dropped = info[3:]
        ses.skd.data['station_codes'][sta.capitalize()]['Dropped'] = ' '.join(sorted(dropped))

def process_channels(line, ses):
    info = line.split()
    freq = '0.00' if len(info) < 3 else info[2]
    if len(info) < 2:
        return
    band = info[0][0:1]
    channel = info[0][2:3]

    data = ses.skd.data
    if band not in data['channels']:
        data['channels'][band] = {}
    data['channels'][band][channel] = [info[0], freq]

def process_qcodes(line, ses):

    data = ses.skd.data
    if line.startswith('Qcod'):
        data['QcodesHdr'] = line.split()[1:]
    else:
        info = re.findall('\d+', line)
        if len(info) == 21 and ':' in line:
            line = line.strip()
            qcodes = data['Qcodes']
            if line[0:1] not in data['station_keys'] or line[1:2] not in data['station_keys']:
                return
            fr = data['station_keys'][line[0:1]]['code']
            to = data['station_keys'][line[1:2]]['code']

            if fr not in qcodes.keys():
                qcodes[fr] = {}
            if to not in qcodes.keys():
                qcodes[to] = {}
            if to not in qcodes[fr]:
                qcodes[fr][to] = {'X': {}, 'S': {}}
            if fr not in qcodes[to]:
                qcodes[to][fr] = {'X': {}, 'S': {}}

            band = line[3:4]
            info = iter(info)
            for hdr in data['QcodesHdr']:
                qcodes[fr][to][band][hdr] = qcodes[to][fr][band][hdr] = int(next(info))


    #    print(band + ',' + fr['id'] + ',' + to['id'] + ',' + str(info))

def process_none(line, ses):
    pass


def process_snr_ratio(line, ses):
    info = line.split()
    if len(info) > 4:
        try:
            fr = ses.skd.data['codes'][info[0][0:1]]
            to = ses.skd.data['codes'][info[0][1:2]]
            n = int(info[2])
            ratio = float(info[1])
            addSNRratio(fr['X'], ratio, n)
            addSNRratio(to['X'], ratio, n)
            n = int(info[4])
            ratio = float(info[3])
            addSNRratio(fr['S'], ratio, n)
            addSNRratio(to['S'], ratio, n)
        except KeyError:
            return

frequencies = {'X': 'ghijklmn', 'S': 'abcdef'}

def process_fourfit(line, ses):
    return

    line = line.strip()
    if line.startswith('*'):
        return

    data = ses.skd.data
    fourfit = data['fourfit']
    if 'if' and 'station' and 'f_group' in line:
        fourfit['station'] = ''
        fourfit['group'] = ''
        info = line.split()
        for i in range(0, len(info)):
            if info[i] == 'station':
                i += 1
                if info[i] not in data['station_keys']:
                    return
                fourfit['station'] = info[i]
            elif info[i] == 'f_group':
                i += 1
                fourfit['group'] = info[i]

    if 'freqs' in line:
        info = line.split('freqs')[1]

        freqs = ''.join(info.split())
        group = fourfit['group']
        station = fourfit['station']
        if not station:
            return
        dropped = fourfit['dropped']

        p = [letter for i, letter in enumerate(frequencies[group]) if letter not in freqs]
        p += [freqs[i - 1:i + 1] for i, letter in enumerate(freqs) if letter == '+' or letter == '-']
        if len(p) > 0:
            if station not in dropped:
                dropped[station] = []
            for ch in p:
                dropped[station].append(get_channel(ch, frequencies[group], data['channels'][group]))
            #print('Missing {} : {} {}'.format(station, str(p), data['codes'][station]['drop']))


def process_header(line, ses):
    if line.startswith('SESSNAME'):
        name = line.split()[1].strip().lower()
        if name != ses.id:
            notify('correlator report', name, 'Not expected name!', quit=True)
    elif line.startswith('CORRE'):
        info = line.split()
        name = ses.correlator if len(info) == 1 else info[1].strip()
        if name == 'WACO':
            name = 'WASH'
        if name != ses.correlator:
            notify('correlator name', name, 'Not expected code!')
    elif line.startswith('CORRTIME'):
        ses.skd.data['session']['correlated'] = line.split()[1].replace('/', '-')


def read_report(ses):

    data = ses.skd.data

    data['channels'] = {}
    data['Qcodes'] = {}

    # This added to data for temporary usage
    data['fourfit'] = {'dropped': {}, 'group': '', 'station': ''}

    sections = {'HEADER': process_header, 'SUMMARY': process_summary, 'STATIONNOTES': process_notes,
                'DROPCHANNELS': process_dropped_channels, 'CHANNELS': process_channels, 'QCODES': process_qcodes,
                'SNRRATIOS': process_snr_ratio, 'FOURFITCONTROLFILE': process_fourfit}

    path, _ = ses.file_path('corr')
    process_it = process_none
    if not os.path.exists(path):
        return data

    with TEXTfile(path) as file:
        while file.has_next():
            line = file.line.replace('Â', ' ')
            if not line or not line.strip():
                continue

            if line.startswith('+'):
                section = re.sub('[^A-Z]', '', line.strip()[1:])
                if section not in sections:
                    process_it = process_none
                else:
                    process_it = sections[section]
            else:
                process_it(line, ses)

    for code, lst in data['fourfit']['dropped'].items():
        data['station_keys'][code]['dropped'] = ' '.join(sorted(lst))

    if 'fourfit' in data:
        data.pop('fourfit') #Remove from data structure
    if 'QcodeHdr' in data:
        data.pop('QcodesHdr')#Remove Qcodes header

    return data


def read(path):
    dropped = defaultdict(list)
    with open(path, errors='ignore') as f:
        txt = f.read()
    version = 'V3' if '%CORRELATOR_REPORT_FORMAT 3' in txt else 'V2'
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
                    dropped[sta].extend([ch.replace(',', '') for ch in lst if ch.startswith(('X', 'S'))])

    return version, nchannels if nchannels == 64 else 16, dropped


if __name__ == '__main__':
    import argparse
    from ivsdb import IVSdata, models

    parser = argparse.ArgumentParser(description='Extract SEFDs from logs')
    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('year')

    app.init(parser.parse_args())

    folder = f'/sgpvlbi/projects/stats/{app.args.year}'

    # dropped = defaultdict(list)

    v3 = '%CORRELATOR_REPORT_FORMAT 3'
    url, tunnel = app.get_dbase_info()
    nbr = 0
    with IVSdata(url, tunnel) as dbase:
        for (ses_id, start) in dbase.get_sessions_from_year(app.args.year, ['standard']):
            session = dbase.get_session(ses_id)
            if (cmt := Path(folder, f'{ses_id}.cmt')).exists() and session.db_folder.exists():
                nbr += 1
                for file in Path(session.db_folder, 'History').glob('*V000_kMk4.hist'):
                    version, nchannels, dropped = read(file)
                    print(f"{nbr:3d} {ses_id:10s} {version} {nchannels} {dropped}")

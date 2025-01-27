from os import listdir
from os.path import isfile, join, splitext
from utils.files import TEXTfile
from utils import toInt
import re

def read_analysis_report(path, data):

    print(path)
    with TEXTfile(path) as file:
        # Read Statistics Session
        if file.readUntil(key_word='session statistics', lower=True) and file.readUntil(key_word='Observations'):
            line = file.line
            data['scheduled'] = int(re.findall('\d+',line)[0])  # read scheduled obs
            if 'corrLoss' not in data:
                if file.readUntil(key_word=' recoverable'):
                    recovered = int(re.findall('\d+', file.line)[0])  # read correlated obs
                data['corrLoss'] = 1.0 - recovered / data['scheduled']
            if file.readUntil(key_word=' used'):
                data['used'] = int(re.findall('\d+', file.line)[0])  # read scheduled obs
        if file.readUntil(key_word='station performance', lower=True):
            while file.has_next():
                line = file.line
                if '---------------' in line:
                    break
                if len(line) > 60:
                    name = line[2:17].strip()
                    if name in data['station_names']:
                        #print(line)
                        sta = data['station_names'][name]
                        sta['sched'] = sta['corr_sched'] = toInt(line[20:29])
                        sta['corr_used'] = sta['used'] = toInt(line[47:56])
                        #print('{} {}'.format(line[47:56], sta['used']))
                        sta['to'] = {}
                        sta['removed'] = False
                        sta['percent'] = 0
                        sta['alive'] = sta['sched'] > 0

        return data
        if file.readUntil(key_word='Baseline Performance'):
            while file.has_next():
                line = file.line
                if not line or '---------------' in line:
                    break
                if len(line) > 65:
                    sta = line[2:19].split('-')
                    if len(sta) < 2:
                        continue
                    fname = sta[0]
                    tname = sta[1]
                    if len(sta) == 4:
                        fname = sta[0] + '-' + sta[1]
                        tname = sta[2] + '-' + sta[3]
                    elif 'VLBA' in sta[1]:
                        fname = sta[0] + '-' + sta[1]
                        tname = sta[2]
                    elif len(sta) > 2:
                        fname = sta[0]
                        tname = sta[1] + '-' + sta[2]



                    fr = data['station_names'][fname.strip()]
                    to = data['station_names'][tname.strip()]
                    nSched = toInt(line[25:34])
                    nUsed = toInt(line[52:62])
                    if nUsed > 0:
                        fr['to'][to['code']] = {'code': to['code'], 'sched': nSched, 'used': nUsed}
                        to['to'][fr['code']] = {'code': fr['code'], 'sched': nSched, 'used': nUsed}
                    else:
                        fr['corr_sched'] -= nSched
                        to['corr_sched'] -= nSched

    return data


def readDataSolution(path, data, stats=False):
    with TEXTfile(path) as file:
        if file.readUntil(key_word='Baseline Statistics'):
            while file.has_next():
                line = file.line
                if 'Constraint Statistics' in line:
                    break

                if len(line) > 24 and 'No Data' not in line:
                    name_fr = line[1:9].strip()
                    name_to = line[10:18].strip()
                    if name_fr in data['station_names'] and name_to in data['station_names']:
                        fr = data['station_names'][name_fr]
                        to = data['station_names'][name_to]
                        n = toInt(line[18:23].replace('/', ''))
                        if 'deselected' in line:
                            n = -n
                        fr['used'] += n
                        to['used'] += n
                        data['used'] += n
                        print('{} {} {:4d} {:4d} {:4d}'.format(fr['code'], to['code'], n, fr['used'], to['used']))
                        fr['corr_used'] = fr['used']
                        to['corr_used'] = to['used']


    return data


def doNothing(line, data):
    return

def processStats(line, data):
    if not line.strip():
        return
    line = line.replace('>', ' ').replace('<', ' ')
    info = line.split()
    sta = data['station_names'].get(info[0], None)
    if sta:
        x = sta['X']['SEFD']['STATS']
        s = sta['S']['SEFD']['STATS']
        for i in range(1, 8):
            s.append(toFloat(info[i]))
            x.append(toFloat(info[i+7]))

def processSEFD(line, data):
    if not line.strip():
        return
    info = line.split()
    sta = data['station_names'].get(info[0], None)
    if sta:
        x = sta['X']['SEFD']
        s = sta['S']['SEFD']
        s['measured'] = toFloat(info[1])
        s['predicted'] = toFloat(info[2])
        x['measured'] = toFloat(info[3])
        x['predicted'] = toFloat(info[4])

def readSNRanal(path, data):

    process = doNothing

    station = processStats
    with TEXTfile(path) as file:
        while file.has_next():
            line = file.line
            header = line.upper()
            if header.strip() == 'SEFD':
                station = processSEFD
            elif 'S-BAND' in header or 'X-BAND' in header:
                if 'STATION' in header:
                    process = station
                else:
                    process = doNothing
            else:
                process(line, data)

    return data

if __name__ == '__main__':

    from ivsdb import IVSdb, fixe_dates
    from utils import load_options, makePath
    from utils_file import ftpListing, ftpPut
    from security import get
    import os

    def check_listing(center, server, wd, files):
        err = {'msg': None}
        lines = ftpListing(server, '', '', wd, err)
        if err['msg']:
            return err

        for line in lines:
            for file in files.values():
                if line.endswith(file['ext']):
                    file['list'].append({'center': center, 'line': line})  # info[8][-6:-4].capitalize())
                    break
        return None

    def check_cddis(session, files):

        cddis = 'cddis'
        server = session.config['data centers'][cddis]
        wd = makePath(session.config['aux'], session.year(), session.id)
        err = check_listing(cddis, server, wd, files)
        return err == None

    def to_lower(name):
        return name.lower()

    def get_analysis_files(config, session):

        server = config['cddis']

        #  Check if file exists
        for code in ['analysis', 'solution']:
            for index in range(1,4):
                local = session.file_path(code, index)
                if not os.path.exists(local):
                    # Check if on ivscc
                    remote = session.file_path_remote(code, server, index)
                    if getFiles(server, local, remote):
                        print('{} transfered!'.format(os.path.basename(local)))

    def get_missing_list(session, files, code):

        missing = []
        for index in range(1, 4):
            report = session.file_path(code, index)
            name = os.path.basename(report).lower()
            if os.path.exists(report):
                found = False
                for file in files[code]['list']:
                    line = file['line'].lower()
                    if name in line:
                        found = True
                if not found:
                    missing.append(report)
        return missing

    def get_missing_files(session):
        files = {'analyst': {'ext': 'analyst.txt', 'list': []}, 'analysis': {'ext': 'report.txt', 'list': []},
                 'solution': {'ext': 'spoolfile.txt', 'list': []}}
        missing = []
        if check_cddis(session, files):
            for file in files['analyst']['list']:
                if session.id in file['line']:
                    return missing

            missing += get_missing_list(session, files, 'analysis')
            missing += get_missing_list(session, files, 'solution')

        return missing

    def upload(config, session):

        missing = get_missing_files(session)

        bkg = config['data centers']['bkg']
        user, passw = (get('bkg').split(':'))
        sent = ftpPut(bkg, user, passw, missing, to_lower)
        for name in sent:
            print('{} uploaded'.format(name))



    opt, config, param = load_options()

    server = config['cddis']

    start, end = fixe_dates(param[0], param[1])


    process = upload if 'bkg' in opt else get_analysis_files

    with IVSdb() as db:
        sessions = db.get_sessions(['IVS', 'INT'], start, end)
        for session in sessions:
            session.config = config
            process(config, session)






def get_analysis_files(ses):
    files = {'report': {}, 'spoolfile': {}}
    for file in listdir(ses.folder):
        if isfile(join(ses.folder,file)) and 'analys' in file:
            filename, _ = splitext(file)

            try:
                id, center, _, code = filename.split('-')[:4]
                center = center.lower()
                if center not in files[code] or file > files[code][center]:
                    files[code][center] = file
            except:
                #print('!!! {} {} {} {}'.format(id, center, code, file))
                files['report']['other'] = file

    for center in ['ivs', 'nasa', 'usno', 'other']:
        if center in files['report']:
            return files['report'][center], files['spoolfile'].get(center, None)
        elif center in files['spoolfile']:
            return None, files['spoolfile'][center]
    return None, None


def read_report(ses):
    print('Read analysis', ses)
    report, spool = get_analysis_files(ses)
    print(report)
    if not report:
        return False
    read_analysis_report(join(ses.folder,report), ses.skd.data)
    return True

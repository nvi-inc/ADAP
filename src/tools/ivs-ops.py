import re
from operator import itemgetter

from email import message_from_bytes
from _collections import defaultdict

from utils import readDICT
from utils.gmail import Gmail
from ivsdb import IVSdata
"""
+------+-------+------+-------+-------------+---------------------+
| code | words | oper | title | description | updated             |
+------+-------+------+-------+-------------+---------------------+
| 0    |       | Y    | READY |             | 2019-06-04 20:31:11 |
| 1    |       | Y    | START |             | 2019-06-04 20:31:11 |
| 2    |       | Y    | STOP  |             | 2019-06-04 20:31:11 |
| 3    |       | D    | OTHER |             | 2019-06-04 20:31:11 |
| 4    |       | Y    | WIKI  |             | 2019-06-04 20:31:11 |
| A    |       | D    | OTHER |             | 2019-06-04 20:31:11 |
| C    |       | N    |       |             | 2019-06-04 20:31:11 |
| N    |       | N    |       |             | 2019-06-04 20:31:11 |
| S    |       | N    |       |             | 2019-06-04 20:31:11 |
| U    |       | N    |       |             | 2019-06-04 20:31:11 |
+------+-------+------+-------+-------------+---------------------+

"""
msg_type = 'ready|start|begin|stop|complete|finish'
msg_report = 'correlation|analysis'

def extract_ids(subject):
    title = subject
    stations = readDICT('/sgpvlbi/progs/adap/config/aliases.toml')
    words = subject.split()
    url = app.secret('DataBase', app.args.db)

    found = {}
    with IVSdata(url) as dbase:
        ids = dbase.get_sessions_from_names(words)
        ids.extend(dbase.get_sessions_from_ids(words))
        subject = re.sub('|'.join(ids), '', subject)
        codes = re.findall(msg_type, subject)
        subject = re.sub(msg_type, '', subject)
        for ses_id in list(set(ids)):
            ses = dbase.get_session(ses_id)
            if ses.type == 'intensive':
                found[ses_id] = {'codes': ['INT']}
                continue
            network = [get(sta_id.capitalize(), None) for sta_id in ses.stations]
            network = sorted(network, key=itemgetter('order', 'code'))
            lst = []
            for station in network:
                for alias in station['aliases']:
                    if alias in subject:
                        lst.append(station['code'])
                        subject = subject.replace(alias, '')
                        break
            found[ses_id] = {'codes': codes, 'stations': lst}

    return found

def test_digits():
    msg = '11021 128 41021'
    url = app.secret('DataBase', app.args.db)
    with IVSdata(url) as dbase:
        for word in re.findall('[0-9]+', msg):
            if len(word) > 2:
                ids = [code for code in dbase.get_sessions_from_digits(f'%{word}') if re.match(f'[a-zA-Z]+{word}',code)]
                print(word, ids)


def get_session(dbase, subject, sta_list):
    words = subject.split()
    if not (ses_ids := dbase.get_sessions_from_ids(words)):
        ses_ids = dbase.get_sessions_from_ids([w[:-2] for w in words if w.endswith(sta_list)])
    if not ses_ids:
        ses_ids = dbase.get_sessions_from_names(words)
    if not ses_ids and (digits := [f'%{w1}' for w in words if (w1 := re.sub(r'[^0-9 ]', '', w)) and len(w1) > 4]):
        ses_ids = dbase.get_sessions_from_digits(digits)
    return ses_ids


def is_valid_message(msg):
    subject = msg['Subject']
    if not re.findall(r'\[IVS\-[a-zA-Z]+\]', subject, re.IGNORECASE):
        return False, None  # Not an IVS message
    # Test if correlator or analysis report
    if re.findall('correl|analys', subject, re.IGNORECASE):
        return True, None  # Not a station message but can mark as read

    # Clean subject
    subject = re.sub(r'[IVS\-[a-zA-Z]+]|fw\:|message|re\:|[.,!?:;]', ' ', subject).lower()
    return True, ' '.join(re.sub(r'[^a-z0-9 ]', '', subject).split())


if __name__ == '__main__':
    import argparse
    from utils import app

    parser = argparse.ArgumentParser( description='Logger for ADAP software.' )
    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-a', '--account', help='gmail account', default='ivsop', required=False)
    parser.add_argument('-l', '--labels', help='label name', default=['INBOX'], nargs='+', required=False)
    parser.add_argument('filters', help='filters to apply', nargs='+')

    args = app.init(parser.parse_args())

    credentials = '/sgpvlbi/progs/config/adap/ivsop-1.json'
    stations = readDICT('/sgpvlbi/progs/config/adap/stations.toml')
    sta_list = tuple([sta.lower() for sta in list(stations.keys())])

    print(args.labels)

    db_url, tunnel = app.get_dbase_info()
    with IVSdata(db_url, tunnel) as dbase, Gmail(credentials) as gmail:
        isOPS, notOPS = gmail.labels['IVS-ops'], gmail.labels['NOT-ops']
        sessions = defaultdict(list)
        for name in args.labels:
            label = gmail.labels.get(name, 'INBOX')
            for uid in gmail.get_uids([label], filters=args.filters):
                msg = gmail.get_msg(uid)
                valid, subject = is_valid_message(msg)
                if subject and (ids := get_session(dbase, subject, sta_list)):
                    [subject for ses_id in ids if (subject := subject.replace(ses_id, ''))]
                    [sessions[id].append(subject) for id in ids]
                continue
                flags = [isOPS] if valid else [notOPS]

                if label == 'INBOX':
                    gmail.mark_as_read(uid, flags=flags)
                if subject:
                    if found := extract_ids(subject):
                        for ses_id, info in found.items():
                            if 'INT' in info['codes']:
                                print(f'{ses_id} is intensive')
                            else:
                                print(f'{ses_id} {info["codes"]} {info["stations"]} {uid}')
                    else:
                        print('No session found')

    for id, subjects in sessions.items():
        [print(id, subject) for subject in subjects]


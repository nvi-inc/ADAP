from utils import app
from datetime import datetime
from aps import APS
import re
import os


def test(path):
    print(f'Reading {path}')
    with open(path) as eo:
        for line in eo:
            line = line.rstrip()
            if not line.startswith('#') and len(line.split()) != 30:
                print(line)


def fix_sessions(path):
    def clean_line(text):
        nbr_corrected = 0
        while True:
            if len(parts := text[1:].split()) > 31:
                delimiter = re.sub(r'\W+', '', parts[30])
                last = text.split(delimiter)[1]
                lines.append(text.replace(last, ''))
                text = last
                nbr_corrected += 1
            else:
                lines.append(text)
                break
        return nbr_corrected

    def key(string):
        mjd, _, ses_id, *_ = string[1:].split()
        return f'{mjd}-{ses_id}'

    print(f'Reading {path}')
    with open(path) as eop, open(os.path.join('/tmp', os.path.basename(path)), 'w') as tmp:
        nbr_corr = 0
        lines = []
        for line in eop:
            line = line.rstrip()
            if line.startswith('#'):
                print(line, file=tmp)
            elif len(line[1:].split()) > 31:
                nbr_corr += clean_line(line)
            else:
                lines.append(line.rstrip())

        lines = sorted(lines, key=key)
        print('\n'.join(lines), file=tmp)

    print('Corrected', nbr_corr, 'lines')

    print(f'Testing {os.path.join("/tmp", os.path.basename(path))}')
    with open(os.path.join('/tmp', os.path.basename(path))) as tmp:
        old_key = None
        last_line = ''
        for index, line in enumerate(tmp, 1):
            if not line.startswith('#'):
                if last_line.startswith('#'):
                    print('comment', index, line[:60])
                new_key = key(line)
                if old_key and new_key < old_key:
                    print('unordered', index, line[:60])
                old_key = new_key
            last_line = line
        print(index, 'lines were tested')


def get_sessions(path):
    print(f'Reading {path}')
    with open(path) as eop:
        for line in eop:
            if not line.startswith('#') and len((info := line[1:].split())) > 31:
                last = line.split(info[30])[1].rstrip()
                first = line.replace(last, '').rstrip()
                print(first)
                print(last)

    with open(path) as eop:
        sessions = [line[1:].split()[2] for line in eop if not line.startswith('#')]
    return set(sessions)


def process_missing_sessions(arguments, processed, master, action, initials):
    dbase = app.get_dbase()
    start, end = datetime(2022, 1, 1), datetime.utcnow()
    index = 1
    for ses_id in dbase.get_sessions(start, end, [master]):
        if ses_id not in processed:
            session = dbase.get_session(ses_id)
            if (aps := session.file_path('aps')).exists():
                items = readDICT(aps)
                if done := items['Actions'][action]['done'] and items['Submissions']['SUBMIT-EOPI']['done']:
                    arguments.param = ses_id
                    # aps = APS(arguments)
                    # aps.run_process('standalone', initials)
                    # aps.run_process(action, initials)
                    print(index, ses_id, done)
                    index += 1


if __name__ == '__main__':

    import argparse
    from utils import readDICT

    parser = argparse.ArgumentParser(description='Tools ')
    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-opa', '--opa_config', help='opa lcl file ', default='', required=False)
    parser.add_argument('param', help='uploaded filenames')

    args = app.init(parser.parse_args())

    master, file, action, initials, eop = ('intensive', 'EOPT_FILE', 'EOPM', 'IK', 'EOPM_FILE') \
        if args.param.startswith('int') else ('standard', 'EOPB_FILE', 'EOPS', 'SB', 'EOPS_FILE')

    info = readDICT(app.Applications.APS.get(master))

    # test(info[eop])
    # fix_sessions(info[file])
    process_missing_sessions(args, get_sessions(info[file]), master, action, initials)


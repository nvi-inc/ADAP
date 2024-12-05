from utils.files import TEXTfile
from utils import app, readDICT
import os
import pwd

class Test:
    def __init__(self):

        self.folders.sessions
        # Read vgosDbProcLogs config file
        setup = self.read_config('vgosDbProcLogs')['Setup']
        self.master_folder = setup['path2masterfiles']
        self.session_folder = os.path.join(setup['path2sessionfiles'], str(self.year), self.code)

def read_config(self, app_name):
    # Read config file
    path = os.path.join(pwd.getpwuid(os.getuid()).pw_dir, '.config', 'NASA GSFC', app_name+'.conf')
    return readDICT(path)

# Read master-format.txt file to extract column names
def read_master_format(path):
    header = {}
    if not os.path.exists(path):
        return header

    with TEXTfile(path) as f_in:
        f_in.readUntil('Field    Title     Format')
        while(f_in.has_next()):
            line = f_in.line
            if line and line[0:5].strip(): # Find field information
                try:
                    field = int(line[0:5].strip())
                    title = line[5:18].strip()
                    #read next line to see title has second line
                    if f_in.has_next():
                        second_part = f_in.line[5:18].strip()
                        if second_part:
                            title = '{}_{}'.format(title, second_part)
                    header[title] = field
                except Exception as e:
                    print('{:3d} {}'.format(f_in.line_nbr, str(e)))
    return header

# Read master file and extract session using db_name
def get_vgosdb(path, db_name, header):

    db_name = db_name.upper()
    date = db_name[2:7]
    db_code = db_name[7:9]
    date_id = header['DATE']
    db_code_id = header['DBC_CODE']
    if os.path.exists(path):
        with TEXTfile(path) as f_in:
            while f_in.has_next():
                if not f_in.line.startswith('|'):
                    continue
                ses = list(map(str.strip, f_in.line.split('|')))
                if ses[date_id] == date and ses[db_code_id] == db_code:
                    return ses
    return []

# Read master file and extract session using session code
def get_session(path, ses_code, header):

    code_id = header['SESSION_CODE']
    if os.path.exists(path):
        with TEXTfile(path) as f_in:
            while f_in.has_next():
                if not f_in.line.startswith('|'):
                    continue
                ses = list(map(str.strip, f_in.line.split('|')))
                if ses[code_id] == ses_code:
                    return ses
    return []


def vgosDb_dump(db_name):
    folder = os.path.join(app.vgosDBs_folder, db_name)



if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser(description='Update web pages')
    parser.add_argument('format', help='index file')
    parser.add_argument('master', help='index file')
    parser.add_argument('db_name', help='index file')

    args = app.init(parser.parse_args())

    header = read_master_format(args.format)
    print(header)
    ses = get_vgosdb(args.master, args.db_name, header)
    print(ses)
    ses_id = ses[header['SESSION_CODE']]
    print(ses_id)
    ses = get_session()


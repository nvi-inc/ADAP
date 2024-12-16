import os
from datetime import datetime
import shutil

from utils import app
from utils.mail import build_message, send_message
from utils.servers import load_servers, get_server, DATACENTER
from tools import record_submitted_files
from ivsdb.models import UploadedFile


class UpdateAprioriEOP:

    USER = 'crontab'
    APPNAME = 'update_apriori_eop'

    def __init__(self, args):
        app.init(args)

        self.apriori_eop = app.load_control_file(name=app.ControlFiles.AprioriEOP)[-1]
        self.files = {}

    def extract_filenames(self, path):
        with open(path) as script:
            for line in script:
                if 'SET CONFIG' in line.upper():
                    config = line.split('=')[-1].strip()
                    break
            else:
                print('Could not find CONFIG')
                return

        ext, ext2 = None, None
        with open(config) as file:
            for line in file:
                if line.startswith('# FILEXT:'):
                    ext = line.split(':', 1)[-1].strip()
                elif line.startswith('# FILEXT2:'):
                    ext2 = line.split(':', 1)[-1].strip()
                elif line.startswith('# FILOUT:'):
                    self.files[line.split(':', 1)[-1].strip()] = ext
                elif line.startswith('# FIL500:'):
                    self.files[line.split(':', 1)[-1].strip()] = ext2

    @staticmethod
    def get_last_date(path):
        last = datetime(1970, 1, 1)
        if os.path.exists(path):
            with open(path) as file:
                for line in file:
                    if line[16] == 'I':
                        last = datetime.strptime(line[:6].replace(' ', '0'), '%y%m%d')
        return last.date()

    def validate_date(self, path):
        try:
            last = self.get_last_date(self.files.get(path, '__NO__'))
            ymd = self.grep(path, '# Last date with real data:').split(':')[-1].strip()
            return datetime.strptime(ymd, '%Y.%m.%d').date() >= last
        except Exception:
            return False

    @staticmethod
    def grep(path, words):
        ans, err = app.exec_and_wait(f'grep -m 1 \"{words}\" {path}')
        return ans

    def notify(self, action, problem):
        title = '** URGENT ** final USNO erp files not updated'
        message = f'{action}\n\n{problem}'
        details = self.apriori_eop['Notifications']
        msg = build_message(details['sender'], details['recipients'], title, text=message, urgent=True)
        send_message(details['server'], msg)

    def move_failed_upload(self, files):
        folder = app.Applications.VLBI['failed_upload']
        names = []
        for file in files:
            shutil.copy(file, folder)
            names.append(os.path.basename(file))
        record_submitted_files([UploadedFile(name, self.USER, self.APPNAME, 'waiting') for name in names])

    def exec(self):
        script = self.apriori_eop['Script']['path']
        self.extract_filenames(script)
        ans, err = app.exec_and_wait(script)
        files = [line.split(':')[-1].strip() for line in ans.splitlines() if 'Writing:' in line]
        # Test if last data in usno files has been updated as of the date in input file
        if files and all([self.validate_date(file) for file in files]):
            if failed := self.upload(files):
                self.notify('cddis upload failed!', '\n'.join([f'Could not upload {file}' for file in failed]))
        else:  # Something wrong
            self.notify(f'Script {script} returned:', f'{ans}\n{err}')

    def upload(self, files):
        load_servers()
        testing = self.apriori_eop.get('Testing', False)
        to_submit = set([os.path.basename(file) for file in files])  # Get name of file
        for i in range(5):  # Try few times before sending error message (sometime the connection hangs)
            with get_server(DATACENTER, 'cddis') as server:
                if uploaded := server.upload(files, testing=testing):
                    record_submitted_files([UploadedFile(name, self.USER, self.APPNAME, 'ok') for name in uploaded])
                    if not (to_submit := to_submit - set(uploaded)):
                        print('All submitted')
                        break  # No files left to submit

        if (failed := [file for file in files if os.path.basename(file) in to_submit]) and not testing:
            self.move_failed_upload(failed)
        return failed


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Execute script updating apriori EOP files')
    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)

    prc = UpdateAprioriEOP(parser.parse_args())
    prc.exec()



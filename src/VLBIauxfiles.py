import shutil
import os
import re
import traceback
from tempfile import NamedTemporaryFile

from utils import app
from utils.servers import get_server, load_servers, DATACENTER
from utils.files import get_md5sum, chmod, remove
from rmq import Worker
from ivsdb import IVSdata
from vgosdb.correlator import CorrelatorReport


# Class to download aux files on local server
class AuxFiles(Worker):

    def __init__(self):
        super().__init__()

        # Make filters for aux files
        self.filters = self.make_filters(app.Applications.AUXfiles.get('Filters', {}))

        # Load some specific options for VLBI application
        self.can_download = app.check_server_capability('can_download')
        self.set_start_time('now')

        self.downloader = {'aux_corr': self.download_corr}

    # Using regex definitions in server control file, create filters to detect file types.
    @staticmethod
    def make_filters(regexes):
        filters = {key: re.compile(regex).match for key, regex in regexes.items()}
        filters['-'] = re.compile(r'.').match  # Not found
        return filters

    # Download correlator report to local server
    def download_corr(self, center, rpath, lpath, checksum=False):
        try:
            # Make temporary file for download
            path = NamedTemporaryFile(delete=False).name
            # Download file
            load_servers(DATACENTER)
            with get_server(DATACENTER, center) as server:
                rpath = os.path.join(server.root, rpath)
                ok, rmd5sum = server.download(rpath, path)
            return CorrelatorReport(path).save(lpath) if ok else (ok, rmd5sum)

        except Exception as err:
            self.notify(f'Error downloading {rpath}\n{str(err)}')
            return False, str(err)

    # Download file to local server and replace if checksum not the same
    def download(self, center, rpath, lpath, checksum=False):
        try:
            # Make temporary file for download
            path = NamedTemporaryFile(delete=False).name if checksum else lpath
            # Download file
            load_servers(DATACENTER)
            with get_server(DATACENTER, center) as server:
                rpath = os.path.join(server.root, rpath)
                ok, rmd5sum = server.download(rpath, path)

            if not ok or not checksum:
                chmod(lpath)
                return ok, rmd5sum
            # Test if it should be replaced
            if get_md5sum(lpath) == rmd5sum:
                remove(path)
                return False, 'MD5 same'  # Do not process since it is the same file
            # Replace old with tmp file
            shutil.move(path, lpath)
            chmod(lpath)
            return ok, 'overwrite it'
        except Exception as err:
            self.notify(f'Error downloading {rpath}\n{str(err)}')
            return False, str(err)

    # Extract file type using pre-defined filters
    def get_file_type(self, name):
        for key, match in self.filters.items():
            if found := match(name):
                if key == '-':
                    break
                sta_id = found['stnid'] if key == 'sta' else None
                overwrite = False if key == 'rep' and found['timestamp'] else True
                return f'aux_{found["type"]}', found['expid'], sta_id, overwrite

        return None, None, None, False

    # Process file
    def process_file(self, center, name, rpath, timestamp):
        code, ses_id, sta_id, overwrite = self.get_file_type(name)
        if not code:
            self.warning(f'{name} from {center} not processed [code is None]')
            return

        update, processed, msg = False, False, 'None'
        url, tunnel = app.get_dbase_info()
        with IVSdata(url, tunnel) as dbase:
            if ses := dbase.get_session(ses_id):  # Found session id in data base
                lpath = os.path.join(ses.folder, name)
                download = self.downloader.get(code, self.download)
                if not os.path.exists(lpath):
                    if self.can_download:
                        processed, msg = download(center, rpath, lpath, False)
                    else:
                        processed, msg = True, f'{name} not downloaded on this server'
                elif not overwrite:
                    update, processed, msg = True, False, 'cannot overwrite'
                elif not os.access(lpath, os.W_OK):
                    msg = 'access problem'
                    self.notify(f'{name} cannot be updated due to privileges')
                elif self.can_download:
                    processed, msg = download(center, rpath, lpath, True)
                else:
                    processed, msg = True, f'{name} not downloaded on this server'
            if processed:
                self.publish(code, f'{name},{ses_id},{sta_id}')
                self.info(f'downloaded {name} from {center} [{code}] {msg}')
                update = True
                dbase.update_recent_file(name, timestamp)
            else:
                self.info(f'{name} from {center} not processed [{code}] {msg}')
                if 'MD5 same' in msg:
                    update = True
            if update:
                dbase.update_recent_file(name, timestamp)

    # Process message from rmq queue
    def process_msg(self, ch, method, properties, body):
        try:
            center, name, rpath, timestamp = body.decode('utf-8').strip().split(',', 3)
            self.process_file(center, name, rpath, timestamp)
        except Exception as err:
            self.notify(f'problem {name} {center}\n{str(err)}\n{traceback.format_exc()}')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Logger for ADAP software.' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-q', '--queue', help='queue name', required=True)

    app.init(parser.parse_args())

    worker = AuxFiles()
    worker.monit()

from tempfile import mkdtemp
import shutil
import traceback
import os

from utils import app
from utils.files import is_master, get_md5sum, remove, chmod
from utils.servers import load_servers, get_server, get_centers, DATACENTER
from rmq import Worker
from ivsdb import IVSdata, loaders


class ControlFile(Worker):

    def __init__(self):
        super().__init__()

        self.failures = {}

        # Set start time as now
        self.set_start_time('now')

    # Update database (MySQL) with new file
    def update_database(self, dbase, name, path, timestamp):

        try:
            if is_master(name):
                success = loaders.load_master(dbase, path)
            elif name == 'ns-codes.txt':
                success = loaders.load_ns_codes(dbase, path)
            elif name == 'master-format.txt':
                success = loaders.load_master_format(dbase, path)
            else:  # Probably master notes
                if timestamp:
                    dbase.update_recent_file(name, timestamp)
                self.notify(f'{name} downloaded! Need to check impact on IVSCC database', wait=False)
                return
            if success and timestamp:
                dbase.update_recent_file(name, timestamp)
            self.info(f'updating database with {name} {"was successful" if success else "failed"}')
        except Exception as err:
            if not timestamp or timestamp > self.failures.get(name, 0):
                self.notify(f'Error reading {name}\n{str(err)}\n{traceback.format_exc()}', wait=False)
                if timestamp:
                    self.failures[name] = timestamp

    # Process message sent by scanners
    def process_file(self, center, name, rpath, timestamp):
        self.info(f'processing {name}')

        # Load DataCenter configurations
        center = center if center in load_servers(DATACENTER) else 'cddis'

        # Download in tmp folder with random name
        tpath = os.path.join((tmp_folder := mkdtemp()), name)
        with get_server(DATACENTER, center) as server:
            rpath = os.path.join(server.root, rpath)
            ok, info = server.download(rpath, tpath)
        if not ok:
            self.critical(f'could not download {name}. [{info}')
            return
        url, tunnel = app.get_dbase_info()
        with IVSdata(url, tunnel) as dbase:
            if not timestamp.isdigit():  # Process but do not save
                self.update_database(dbase, name, tpath, None)
            else:  # Process if new file
                lpath = os.path.join(app.VLBIfolders.control, name)
                if os.path.exists(lpath) and get_md5sum(lpath) == info:  # Same file on server
                    dbase.update_recent_file(name, timestamp)
                else:  # New file
                    self.update_database(dbase, name, tpath, int(timestamp))
                    shutil.move(tpath, lpath)  # Update file in control folder
                    chmod(lpath)
                    if is_master(name):
                        ans, err = app.exec_and_wait(f'update_vdb_master {lpath}')
                        if err:
                            self.notify(f'update_vdb_master {lpath} failed\n{ans}\n{err}')
        shutil.rmtree(tmp_folder)

    # Process message from rmq queue
    def process_msg(self, ch, method, properties, body):
        try:
            center, name, rpath, timestamp = body.decode('utf-8').strip().split(',', 3)
            self.process_file(center, name, rpath, timestamp)
        except Exception as err:
            self.notify(f'problem {body.decode()}\n{str(err)}\n{traceback.format_exc()}')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Process control files')

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-q', '--queue', help='queue name', required=True)

    app.init(parser.parse_args())

    worker = ControlFile()
    worker.monit()

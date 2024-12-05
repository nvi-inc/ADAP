import os

from datetime import datetime, timedelta
import pytz

from utils import app
from utils.servers import get_server, load_servers, get_centers, get_config_item, DATACENTER, CORRELATOR
from rmq import RMQclient
from ivsdb import IVSdata, models


class DCscanner(RMQclient):
    reject = ['MD5SUMS', 'SHA512SUMS']

    def __init__(self):
        super().__init__()

        self.too_old = datetime.utcnow().timestamp() - app.args.recent * 86400

    def __enter__(self):
        self.connect()  # Connect to message rmq
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # Check if file is less than 1 day old
    def is_recent(self, timestamp):
        return timestamp > self.too_old

    # Find new files on specific folder on data center
    def find_files(self, options):
        center, folder_id = options
        if center not in load_servers(DATACENTER):
            self.add_error(f'{center} not a valid data center')
            return
        folders = app.Applications.DataCenters['Folders']
        folder_id = folder_id if folder_id in folders else next(iter(folders))
        folder = folders[folder_id]
        status = f'scanning dc {center} {folder}'
        self.start(status)

        # Open data base and server
        url, tunnel = app.get_dbase_info()

        with IVSdata(url, tunnel) as dbase, get_server(DATACENTER, center) as server:
            if not server.connected:
                self.add_error(f'could not connect to {server.url}')
            else:
                # Scan folder
                try:
                    recent = os.path.join(server.scan, folder)
                    for name, path, timestamp in server.walk(recent, self.reject):
                        if self.is_recent(timestamp) and dbase.is_new_file(name, timestamp):  # new file
                            rpath = os.path.join(folder, os.path.relpath(path, recent))
                            # Send to processing worker
                            self.info(f'detected {name} on {center} {timestamp} [{folder_id}]')
                            self.publish(folder_id, f'{center},{name},{rpath},{timestamp}')
                except Exception as err:
                    self.add_error(f'{center} {folder_id} {str(err)}')
        self.stop(f'{status}{ " with errors" if self.has_errors else ""}')


class CORRscanner(DCscanner):
    reject = ['MD5SUMS', 'SHA512SUMS']

    # Filter for Haystack files that is not using standard naming for its vgosdb files
    @staticmethod
    def hays_filter(dbase, server, name):
        session = dbase.get_session(dbase.get_db_session_code(name))
        path = server.file_name.format(year=session.year, session=session.code, db_name=session.db_name)
        return name == os.path.basename(path)

    # Filter for UTAS for removing tar.gz files
    @staticmethod
    def utas_filter(dbase, server, name):
        filename, ext = os.path.splitext(name)
        return ext == '.tgz' and len(filename) == 9

    # Do not filter out any file
    @staticmethod
    def no_filter(dbase, server, name):
        return True

    # Find new vgosDB files on specific correlator site
    def find_files(self, correlator):
        if correlator not in load_servers(CORRELATOR):
            self.add_error(f'{correlator} not valid correlator code' )
            return

        status = f'scanning corr {correlator}'
        self.start(status)
        # Check for filename filter
        filter_name = get_config_item(CORRELATOR, correlator, 'filter', 'no_filter')
        file_filter = getattr(self, filter_name) if hasattr(self, filter_name) else self.no_filter

        # Open data base and server
        url, tunnel = app.get_dbase_info()
        with IVSdata(url, tunnel) as dbase, get_server(CORRELATOR, correlator) as server:
            if not server.connected:
                self.add_error(f'could not connect to {server.url}')
            else:
                root_folder = server.scan if hasattr(server, 'scan') else server.root
                try:
                    for name, path, timestamp in server.walk(root_folder, self.reject):
                        if self.is_recent(timestamp) and dbase.get_db_session_code(name) \
                                and dbase.is_new_file(name, timestamp, tableId=1) \
                                and file_filter(dbase, server, name):
                            self.info(f'detected {name} on {correlator} {timestamp}')
                            self.publish('new-vgosdb', f'{correlator},{name},{path},{timestamp}')
                except Exception as err:
                    self.add_error(f'{correlator} {str(err)}')
        self.stop(f'{status}{" with errors" if self.has_errors else ""}')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Scan data centers or correlators.' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-r', '--recent', help='days for recent file', type=int, default=14, required=False)
    parser.add_argument('-correlator', help='correlator', required=False)
    parser.add_argument('-data', help='data center', nargs='+', required=False)

    args = app.init(parser.parse_args())

    options, Scanner = (args.correlator, CORRscanner) if args.correlator else (args.data, DCscanner)
    with Scanner() as scanner:
        scanner.find_files(options)
        scanner.send_errors()


import os
from datetime import datetime, timedelta
import tempfile
import shutil

from utils import app
from utils.servers import get_server, load_servers, SERVER
from utils.files import chmod
from rmq import Worker


class MERRA2updater(Worker):

    def __init__(self):
        super().__init__()

        # Maximum timeout
        self.exclusive_queue = True # Create an exclusive queue that will delete when finished.
        self.set_start_time(app.args.start, app.args.period, reset_timeout = False)

    # Read bds_summary.txt file and extract valid time for each stations
    def read_summary(self, path):
        fmt = '%Y.%m.%d-%H:%M:%S'
        updated, stations = datetime(1980, 1, 1), {}
        if os.path.exists(path):
            with open(path) as sum:
                for line in sum:
                    if line.startswith('STA:'):
                        data = line.split()
                        stations[data[2]] = datetime.strptime(data[5], fmt)
                    elif line.startswith('LAST_UPDATE:'):
                        updated = datetime.strptime(line.split()[1].strip(), fmt)
        return updated, stations

    # Find missing files
    def find_missing_files(self, model, server_name, summary, folder):
        if not folder or not os.path.exists(folder):
            self.notify(f'Problem downloading {model} files.\nFolder {folder} does not exist')
            return
        if server_name not in load_servers(SERVER):
            self.notify(f'{server_name} not valid server name')
            return

        # Read summary file on our server
        name = summary
        bds_file = os.path.join(folder, summary)
        last, old = self.read_summary(bds_file)
        day_one = datetime(1980, 1, 1)

        # Upload bds_summary.txt from massloading server
        basename, ext = os.path.splitext(name)
        tpath = tempfile.NamedTemporaryFile(prefix=basename+'_', suffix=ext, delete=False).name
        nbr, total, errors = 0, 0, []
        with get_server(SERVER, server_name) as server:
            # Function to build remote path
            rpath = os.path.join(server.root, model, summary)
            if not server.download(rpath, tpath):
                errors.append(f'Could not download {summary}')
            else:
                updated, stations = self.read_summary(tpath)
                total = len(stations)
                if updated > last:  # Update only if bds_summary is newer
                    # Copy all station files in folder
                    for station, mdate in stations.items():
                        if mdate > old.get(station, day_one):
                            filename = f'{station}.bds'
                            rpath, lpath = os.path.join(server.root, model, filename), os.path.join(folder, filename)
                            if server.download(rpath, lpath):
                                nbr += 1
                            else:
                                errors.append(f'Could not download {filename}')
                    # Update bds_summary.txt file in folder
                    shutil.move(tpath, bds_file, copy_function=shutil.copyfile)
                    chmod(bds_file)

        if errors:
            self.notify('Problem downloading {} files.\n{}'.format(model, '\n'.join(errors)))
        self.info(f'downloaded {nbr} {model} files out of {total}. {len(errors)} errors')

    # Process models
    def process_models(self, parent):
        config = app.load_control_file(name=app.ControlFiles.MassLoading)[-1]
        for model, info in config['Model'].items():
            try:
                self.find_missing_files(model, config['server'], config['summary'], info['folder'])
            except Exception as err:
                self.notify(f'{parent} failed {str(err)}')

    # Process msg
    def process_msg(self, ch, method, properties, body):
        self.process_models('process_msg')

    # Process timeout.
    def process_timeout(self):
        self.process_models('process_msg')

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Checks for VMF files.' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-q', '--queue', help='queue name', required=True)
    parser.add_argument('-s', '--start', help='start time', default='now', required=False)
    parser.add_argument('-p', '--period', help='repeat period', type=int, default=1800, required=False)

    app.init(parser.parse_args())

    worker = MERRA2updater()
    worker.monit()

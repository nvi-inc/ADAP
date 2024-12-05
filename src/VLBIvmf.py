import os
from datetime import date, timedelta
from urllib.parse import urljoin
import traceback

from utils import app
from utils.servers import load_servers, get_server, SERVER
from utils.files import chmod
from rmq import Worker


class VMFscanner(Worker):

    def __init__(self):
        super().__init__()

        self.last_date = date(2008, 1, 2)
        # Maximum timeout
        self.exclusive_queue = True # Create an exclusive queue that will delete when finished.
        self.set_start_time(app.args.start, app.args.period, reset_timeout = False)

    # Find the list of missing vmf files
    def find_missing_files(self):
        # Get vmf folder
        folder = app.VLBIfolders.vmf

        # Scan vmf folder to find missing files
        now, delta = date.today(), timedelta(days=1)
        day = self.last_date - delta
        while day <= now:
            year, name = day.strftime('%Y'), day.strftime('%Y%j.vmf3_r')
            os.makedirs(os.path.join(folder, year), exist_ok=True)
            path = os.path.join(folder, year, name)
            if not os.path.exists(path):
                yield path, day, year, name
            # Next day
            day += delta

    # Get missing VMF files
    def get_missing_files(self, center):
        # Do not check files during quiet time
        if self.is_quiet_time():
            return
        center = center if center in load_servers('Server') else app.Applications.VMF['server']
        missing = []
        # Check for missing files on vmf server
        with get_server('Server', center) as server:
            if server.connected:
                for lpath, day, year, name in self.find_missing_files():
                    if server.download(urljoin(server.root, f'{year}/{name}'), lpath)[0]:
                        chmod(lpath)
                        self.info(f'downloaded {name}')
                    else:
                        missing.append(day)
        self.last_date = missing[0]

    # Process msg
    def process_msg(self, ch, method, properties, body):
        try:
            self.get_missing_files(body.decode('utf-8').strip())
        except Exception as err:
            self.notify(f'process_msg failed {str(err)}\n{traceback.format_exc()}')

    # Process timeout.
    def process_timeout(self):
        try:
            self.get_missing_files(app.Applications.VMF['server'])
        except Exception as err:
            self.notify(f'VMF download failed {str(err)}\n{traceback.format_exc()}')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Checks for VMF files.' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-q', '--queue', help='queue name', required=True)
    parser.add_argument('-s', '--start', help='start time', default='now', required=False)
    parser.add_argument('-p', '--period', help='repeat period', type=int, default=1800, required=False)

    app.init(parser.parse_args())

    worker = VMFscanner()
    worker.monit()

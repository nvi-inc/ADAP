import os
from subprocess import Popen, DEVNULL
from collections import deque
import traceback

from utils import app
from utils.servers import load_servers, get_config_item, DATACENTER
from rmq import Worker
from tools import kill_processes


class VLBIdatacenters(Worker):

    def __init__(self):
        super().__init__()

        # Circular counter selecting folder codes
        self.codes = deque(list(app.Applications.DataCenters['Folders'].keys()))
        self.scanner = os.path.join(os.environ['APP_DIR'], app.Applications.DataCenters['scanner'])
        self.exclusive_queue = True  # Create an exclusive queue that will delete when finished.
        # Maximum timeout
        self.set_start_time(app.args.start, app.args.period, reset_timeout = False)

    # Start a scanner for each data center for a specific folder
    def start_datacenter_scanners(self, center, code):

        # Kill processes that have been there for too long
        processes, problems = kill_processes(['VLBIscanner', '-data'])
        if problems:
            self.notify('\n'.join(problems))
        running_processes = {'-'.join(prc.info['cmdline'][-2:]) for prc in processes}

        # Do not start any scan when in quiet time
        if self.is_quiet_time():
            return

        centers = [name for name in load_servers(DATACENTER) if get_config_item(DATACENTER, name, 'scan', None)]
        centers = [center] if center in centers else centers
        for center in centers:
            if f'{center}-{code}' not in running_processes:  # Do not start a scanner for center if already running
                # Start application that will scan the data center for this folder
                Popen(f'{self.scanner} {center} {code}', shell=True, stdin=DEVNULL, stdout=DEVNULL, stderr=DEVNULL)

    # Process msg
    def process_msg(self, ch, method, properties, body):
        try:
            center, code = list(map(str.strip, body.decode('utf-8').split(',')))
            code = code if code in self.codes else self.codes[0]
            self.start_datacenter_scanners(center, code)
        except Exception as err:
            self.notify(f'process_msg failed with {body.decode("utf-8")}\n{str(err)}')

    # Process timeout.
    def process_timeout(self):
        try:
            self.start_datacenter_scanners('all', self.codes[0])
            self.codes.rotate(-1)
        except Exception as err:
            self.notify(f'on timeout failed with {str(err)}\n{traceback.format_exc()}')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Start scanners for each data centers')

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-q', '--queue', help='queue name', required=True)
    parser.add_argument('-s', '--start', help='start time', default='now', required=False)
    parser.add_argument('-p', '--period', help='repeat period', type=int, default=300, required=False)

    app.init(parser.parse_args())

    worker = VLBIdatacenters()
    worker.monit()

import os
import traceback
from subprocess import Popen, DEVNULL

from utils import app
from utils.servers import load_servers, get_config_item, CORRELATOR
from rmq import Worker
from tools import kill_processes


class VLBIcorrelators(Worker):

    def __init__(self):
        super().__init__()

        self.scanner = os.path.join(os.environ['APP_DIR'], app.Applications.Correlators['scanner'])
        # Maximum timeout
        self.exclusive_queue = True # Create an exclusive queue that will delete when finished.
        self.set_start_time(app.args.start, app.args.period, reset_timeout = False)

    # Starts a scanner for each correlator
    def start_scanners(self, center):
        # Kill old processes that are taking too much time
        processes, problems = kill_processes(['VLBIscanner', '-correlator'])
        if problems:
            self.notify('\n'.join(problems))
        running_processes = {' '.join(prc.info['cmdline'][-1:]) for prc in processes}
        # Do not start any scan when in quiet time
        if self.is_quiet_time():
            return

        centers = [name for name in load_servers(CORRELATOR) if get_config_item(CORRELATOR, name, 'scan', None)]
        centers = [center] if center in centers else centers
        for center in centers:
            if center not in running_processes:
                # Start application that will scan the data center for this folder
                Popen(f'{self.scanner} {center}', shell=True, stdin=DEVNULL, stdout=DEVNULL, stderr=DEVNULL)

    # Process message send manually
    def process_msg(self, ch, method, properties, body):
        try:
            center = body.decode('utf-8').strip()
            self.start_scanners(center)
        except Exception as err:
            self.notify(f'process_msg failed with {body.decode("utf-8")}\n{str(err)}')

    # Process timeout.
    def process_timeout(self):
        try:
            self.start_scanners('all')
        except Exception as err:
            self.notify(f'on timeout failed with {str(err)}\n{traceback.format_exc()}')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Start scanners for each correlators.')

    parser.add_argument('-c', '--config', help='config file', default='/sgpvlbi/progs/adap/config/vlbi.toml', required=False)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-q', '--queue', help='queue name', required=True)
    parser.add_argument('-s', '--start', help='start time', default='now', required=False)
    parser.add_argument('-p', '--period', help='repeat period', type=int, default=300, required=False)

    app.init(parser.parse_args())

    worker = VLBIcorrelators()
    worker.monit()

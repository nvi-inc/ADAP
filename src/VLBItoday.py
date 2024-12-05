import os
import traceback
from datetime import datetime, timedelta
from subprocess import Popen, DEVNULL

from utils import app
from ivsdb import IVSdata
from rmq import Worker


# This class find the list of intensives for today and start an Intensive scanner for each session
class Intensives4Today(Worker):

    def __init__(self):
        super().__init__()

        self.scanner = os.path.join(os.environ['APP_DIR'], app.Applications.Intensive['scanner'])

        # Maximum timeout
        self.exclusive_queue = True # Create an exclusive queue that will delete when finished.
        self.set_start_time(app.args.start, app.args.period, reset_timeout=False)

    # Test that utc time not few seconds below 00:00UT
    @staticmethod
    def get_time_limits():
        now = datetime.utcnow()
        tomorrow = datetime(now.year, now.month, now.day) + timedelta(days=1)
        today = tomorrow if (tomorrow - now).total_seconds() < 3600 else datetime(now.year, now.month, now.day)
        return today.strftime('%Y-%m-%d 00:00:00'), today.strftime('%Y-%m-%d 23:59:59')

    # Start script to monitor intensive vgosDB fro specific correlator site
    def monitor_intensive(self, ses_id):
        Popen([f'{self.scanner} {ses_id}'], shell=True, stdout=DEVNULL, stderr=DEVNULL).wait()
        return ses_id

    # Check which intensives will be run today
    def check_intensives(self):
        self.start('checking intensive schedules for today')
        start, end = self.get_time_limits()
        url, tunnel = app.get_dbase_info()
        with IVSdata(url, tunnel) as dbase:
            sessions = [self.monitor_intensive(ses_id) for ses_id in dbase.get_sessions(start, end, ['intensive'])]
        msg = f'monitoring intensives - {" ".join(sessions) if sessions else "None"}'
        self.stop(msg)
        self.notify(msg)

    # Process msg from queue
    def process_msg(self, ch, method, properties, body):
        try:
            self.check_intensives()
        except Exception as err:
            self.notify(f'problems {str(err)}')

    # Process timeout.
    def process_timeout(self):
        try:
            self.check_intensives()
        except Exception as err:
            self.notify(f'problems {str(err)}\n{traceback.format_exc()}')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Check coming intensives for the day.')

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-q', '--queue', help='queue name', required=True)
    parser.add_argument('-s', '--start', help='start time', default='00:05', required=False)
    parser.add_argument('-p', '--period', help='repeat period', type=int, default=86400, required=False)

    app.init(parser.parse_args())

    worker = Intensives4Today()
    worker.monit()

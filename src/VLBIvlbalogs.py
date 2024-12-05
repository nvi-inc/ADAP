import os
import traceback
from datetime import datetime, timedelta

from utils import app
from utils.servers import get_server, load_servers, SERVER
from ivsdb import IVSdata
from rmq import Worker


class VLBAlogs(Worker):

    info_period = timedelta(hours=1)

    def __init__(self):
        super().__init__()

        self.has_logs = []

        self.exclusive_queue = True  # Create an exclusive queue that will delete when finished.
        self.set_start_time(app.args.start, app.args.period, reset_timeout=False)
        self.last_msg = datetime(2000, 1, 1)

    def update_status(self, msg):
        if (datetime.utcnow() - self.last_msg) > self.info_period:
            self.info(msg)
            self.last_msg = datetime.utcnow()

    def get_missing_logs(self):

        now = datetime.utcnow()
        start = now - timedelta(days=14)
        end = now + timedelta(days=14)

        # Get list of missing log files
        missing = {}
        url, tunnel = app.get_dbase_info()
        with IVSdata(url, tunnel) as dbase:
            for ses_id in dbase.get_sessions(start, end, masters=['intensive']):
                if (session := dbase.get_session(ses_id)) and session.has_vlba:
                    for log in ['skd', 'vex', *app.VLBA.logs]:
                        if not session.file_path(log).exists():
                            missing[ses_id] = session
                            break

        if not missing:
            self.update_status('no missing vlba log files')
        else:
            self.update_status(f'checking {",".join(missing.keys())}')
            load_servers()
            with get_server(SERVER, 'vlba') as server:
                for ses_id, session in missing.items():
                    for log in ['skd', 'vex', *app.VLBA.logs]:
                        if not (lpath := session.file_path(log)).exists():
                            rpath = os.path.join(server.root, session.year, ses_id, lpath.name)
                            if server.download(rpath, lpath)[0]:
                                self.info(f'downloaded {lpath.name}')

    # Process timeout.
    def process_timeout(self):
        try:
            self.get_missing_logs()
        except Exception as err:
            self.notify(f'{str(err)}\n{traceback.format_exc()}')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Logger for ADAP software.' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-q', '--queue', help='queue name', required=True)
    parser.add_argument('-s', '--start', help='start time', default='now', required=False)
    parser.add_argument('-p', '--period', help='repeat period', type=int, default=300, required=False)

    app.init(parser.parse_args())

    worker = VLBAlogs()
    worker.monit()


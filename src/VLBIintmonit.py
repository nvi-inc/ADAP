import os
import traceback
from datetime import datetime, timedelta

from utils import app
from utils.servers import get_server, get_aliases, load_servers, CORRELATOR, SERVER
from ivsdb import IVSdata
from rmq import Worker


class VGOSdbMonit(Worker):

    info_period = timedelta(hours=1)
    too_long = timedelta(days=2)
    stop_monit = timedelta(days=4)

    def __init__(self):
        super().__init__()

        # Check if session exist
        url, tunnel = app.get_dbase_info()
        with IVSdata(url, tunnel) as dbase:
            self.session = dbase.get_session(app.args.name)
            if not self.session:
                self.terminate(f'{app.args.name.upper()} is invalid session')

        self.listen_queue = f'{self.session.db_name}-{self.session.code.upper()}'
        self.title = f'{self.session.db_name} ({self.session.code.upper()})'
        self.exclusive_queue = True  # Create an exclusive queue that will delete when finished.
        self.start_monitoring = (self.session.start + timedelta(seconds=self.session.duration))

        self.set_start_time('now', app.args.period)
        self.last_warning, self.last_msg = None, None

    # Information send when application start
    def begin(self):
        start = 'now' if datetime.utcnow() > self.start_monitoring else self.start_monitoring.strftime(' sleeping until (%H:%M)')
        self.logit('BEGIN', f'monitoring {self.title} {start}')

    # Log reason for termination and exit
    def terminate(self, reason):
        if not self.conn:
            self.connect()  # Connect to RabbitMQ to send message
        self.end(f'{self.title} {reason}' if self.session else reason)
        self.exit()

    #  Find vgosdb file from list of server sites
    def find_vgosdb(self, log_it, send_warning):
        centers = get_aliases(CORRELATOR, self.session.correlator.lower())
        centers.extend([center for center in ['bkg', 'cddis', 'opar'] if center not in centers])
        correlators = {}
        for center in centers:
            with get_server(CORRELATOR, center) as server:
                rpath = server.file_name.format(year=self.session.year, ses=self.session.code.lower(),
                                                db_name=self.session.db_name)
                name = os.path.basename(rpath)
                correlators[center] = (name, rpath)
                url = os.path.join(server.root, rpath)
                exists, timestamp = server.get_file_info(url)
                if exists:
                    self.info(f'detected {name} on {center} {timestamp}')
                    self.publish('new-vgosdb', f'{center},{name},{url},{timestamp}')
                    return True
                elif log_it:
                    self.info(f'monitoring {name} {center} {url}')

        if send_warning:
            # Send warning using notification
            monit = '\n'.join([f'{name} {key} {path}' for key, (name,path) in correlators.items()])
            self.notify(f'{self.title} starting monitoring!\n{monit}')

        return False

    # Process message coming from the queue
    def process_msg(self, ch, method, properties, body):
        info = body.decode('utf-8').strip()
        if info == 'status':
            if datetime.utcnow() < self.start_monitoring:
                self.info(f'{self.title} waiting until {self.start_monitoring}')
            else:
                self.info(f'{self.title} monitoring since {self.start_monitoring}')
        elif info in ['not correlated', 'done']:
            self.terminate(info)

    # Process timeout.
    def process_timeout(self):
        errors, now, log_it, send_warning = '', datetime.utcnow(), False, False
        if not self.last_msg:  # First time
            self.last_warning, self.last_msg = now, now
            log_it = True

        if (now - self.start_monitoring) > self.stop_monit:
            # self.notify(f'Stopped monitoring {self.title} after {self.stop_monit} days')
            self.terminate(f'nothing found after {self.stop_monit} days!')
        if (now - self.start_monitoring) > self.too_long:
            self.timeout = 300  # No need to check every minute
        if (now - self.last_msg) > self.info_period:
            log_it, self.last_msg = True, now

        # Look for VGOSdb
        try:
            load_servers()
            if self.find_vgosdb(log_it, send_warning):
                self.terminate('found vgodDb file!')
        except Exception as err:
            msg = f'{self.title} problem {str(err)}'
            self.critical(msg)
            self.notify(f'{msg}\n{traceback.format_exc()}')
            self.terminate('critical error')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Logger for ADAP software.' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-q', '--queue', help='queue name', default= 'none', required=False)
    parser.add_argument('-s', '--start', help='start time', default='now', required=False)
    parser.add_argument('-p', '--period', help='repeat period', type=int, default=60, required=False)
    parser.add_argument('-n', '--name', help='session name', required=True)

    app.init(parser.parse_args())

    worker = VGOSdbMonit()
    worker.monit()

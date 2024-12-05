import os
import traceback

from rmq import Worker
from utils import app
from utils.servers import get_server, load_servers, DATACENTER
from tools import record_submitted_files
from ivsdb.models import UploadedFile


# ADAP application to upload files to cddis
class Uploader(Worker):

    def __init__(self):
        super().__init__()

        self.exclusive_queue = True # Create an exclusive queue that will delete when finished.
        self.set_start_time(app.args.start, app.args.period, reset_timeout = False)

    # Process message in queue
    def process_msg(self, ch, method, properties, body):
        self.submit_files()

    # Process timeout.
    def process_timeout(self):
        try:
            self.submit_files()
        except Exception as err:
            self.notify(f'{str(err)}\n{traceback.format_exc()}')

    # Submit un-submitted files to cddis
    def submit_files(self):
        # Get data center information
        if (center := app.Applications.APS.get('submit_to', 'cddis')) not in load_servers(DATACENTER):
            self.notify(f'Could not upload files to {center}', f'{center} unknown servers')
            return
        # Get files in the failed_upload folder
        folder = app.Applications.VLBI['failed_upload']
        if not (files := {name: path for name, path in [(name, os.path.join(folder, name))
                                                        for name in os.listdir(folder)] if os.path.isfile(path)}):
            return  # No files to upload
        # Upload to data center
        server = get_server(DATACENTER, center)
        # Check if hostname is production
        submitted = []
        for name in server.upload(list(files.values())):
            if name in files:
                os.remove(files[name])
                submitted.append(UploadedFile(name, 'oper', 'uploader', 'ok'))
        record_submitted_files(submitted)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Logger for ADAP software.' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-q', '--queue', help='queue name', required=True)
    parser.add_argument('-s', '--start', help='start time', default='now', required=False)
    parser.add_argument('-p', '--period', help='repeat period', type=int, default=300, required=False)
    parser.add_argument('-t', '--testing', help='test mode', action='store_true')

    app.init(parser.parse_args())
    worker = Uploader()
    worker.monit()

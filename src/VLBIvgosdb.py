from rmq import Worker
from vgosdb.controller import VGOSDBController
from utils import app
from utils.servers import load_servers
from ivsdb import IVSdata


class VGOSdbWorker(Worker):

    def __init__(self):
        super().__init__()

        self.vgosdb = VGOSDBController()

        # Overwrite default functions for sending messages
        self.vgosdb.info = self.info
        self.vgosdb.warning = self.warning
        self.vgosdb.notify = self.notify

        self.set_start_time('now')

    def process_msg(self, ch, method, properties, body):

        load_servers()
        try:
            center, name, rpath, timestamp = body.decode('utf-8').strip().split(',')
            if self.vgosdb.process(center, name, rpath, reject_old=True):
                url, tunnel = app.get_dbase_info()
                with IVSdata(url, tunnel) as dbase:
                    dbase.update_recent_file(name, timestamp, tableId=1)
        except Exception as e:
            self.notify('[{}]\n{}'.format(body.decode('utf-8'), str(e)))


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='VGOSDB preprocessor.' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-q', '--queue', help='queue name', required=True)
    parser.add_argument('-C', '--corr', help='do not download correlator report', action='store_false')
    parser.add_argument('-t', '--test', help='test mode', action='store_true')
    parser.add_argument('-m', '--no_mail', help='no email', action='store_true')

    app.init(parser.parse_args())

    worker = VGOSdbWorker()
    worker.monit()

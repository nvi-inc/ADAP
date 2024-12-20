from utils import app
from rmq import RMQclient


class RefreshMaster(RMQclient):

    def __init__(self):
        super().__init__()

        self.attempts = 0

    def __enter__(self):
        self.connect()  # Connect to message rmq
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def process(self, name):
        center = 'cddis'
        path = f'ivscontrol/{name}'

        self.publish('control', f'{center},{name},{path},reload')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Process control files' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('name', help='master file name')

    args = app.init(parser.parse_args())

    with RefreshMaster() as master:
        master.process(args.name)


from utils import app
from rmq import API


class Bindings(API):

    def __init__(self):
        super().__init__()

        self.connect()

        self.wd_config = app.load_control_file(name=app.ControlFiles.Watchdog)[-1]

    def list(self, name):
        return self.get(f'exchanges/{self.vhost}/{name}/bindings/source')

    def definitions(self):
        return self.get(f'definitions//{self.vhost}/bindings')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Watchdog for ADAP software.')
    parser.add_argument('-c', '--config', help='vlbi config file', required=True)

    app.init(parser.parse_args())

    bindings = Bindings()

    for name in ['LOG', 'VLBI']:
        for info in bindings.list(name)[-1]:
            print(info['source'], info['destination'], info['destination_type'], info['routing_key'])

from utils import app
from rmq import API


class RMQinfo(API):
    def __init__(self):
        super().__init__()

        self.connect()

    def get_vhosts(self):
        ok, vhosts = self.get('vhosts')
        if ok:
            for vhost in vhosts:
                print(f'vhost: {vhost["name"]}')

    def get_queue(self, name):
        ok, items = self.get(f'queues/{self.vhost}/{name}')
        if ok:
            for item in items:
                print(name, item)

    def get_queues(self):
        ok, queues = self.get(f'queues/{self.vhost}')
        return queues if ok else []

    def get_exchanges(self):
        ok, exchanges = self.get(f'exchanges/{self.vhost}')
        return exchanges if ok else []

    def get_bindings(self):
        ok, bindings = self.get(f'bindings/{self.vhost}')
        return bindings if ok else []


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Watchdog for ADAP software.')
    parser.add_argument('-c', '--config', help='vlbi config file', required=True)

    app.init(parser.parse_args())

    info = RMQinfo()
    print('Exchanges')
    for exchange in info.get_exchanges():
        print(f'{exchange["name"]}: {exchange}')
    print('Queues')
    for queue in info.get_queues():
        print(f'{queue["name"]}: {queue}')
    print('Bindings')
    for binding in info.get_bindings():
        print(binding)



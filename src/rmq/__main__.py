from utils import app
from rmq import RMQclient


class RMQbuilder(RMQclient):
    def __init__(self):
        super().__init__()

        self.builder_info = app.load_control_file(name=app.ControlFiles.RMQ)[1]['Build']

    def create_exchanges(self):
        channel = self.conn.channel()
        for name, item in self.builder_info['Exchanges'].items():
            print('Exchange', name, item['args'])
            channel.exchange_declare(exchange=name, **item['args'])
            if 'binding' in item:
                for key in item["binding"]["keys"]:
                    channel.exchange_bind(destination=item['binding']['exchange'], source=name, routing_key=key)

    def create_queues(self):
        channel = self.conn.channel()
        for name, item in self.builder_info['Queues'].items():
            arguments = dict(**item.get('arguments', {}), **{'x-queue-type': 'classic'})
            print('Queue', name, arguments)
            channel.queue_declare(queue=name, durable=True, arguments=arguments)
            if 'binding' in item:
                for key in item["binding"]["keys"]:
                    channel.queue_bind(exchange=item['binding']['exchange'], queue=name, routing_key=key)


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser(description='RabbitMQ information')

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)

    args = app.init(parser.parse_args())

    client = RMQbuilder()
    client.connect()
    client.create_exchanges()
    client.create_queues()

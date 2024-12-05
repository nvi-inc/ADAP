from rmq import connect


def create_exchanges(channel):
    exchanges = {'LOG': 'fanout', 'VLBI': 'direct'}
    for name, exchange_type in exchanges.items():
        channel.exchange_declare(exchange=name, exchange_type=exchange_type, durable=True)
    channel.exchange_bind(destination='LOG', source='VLBI', routing_key='log')


def create_queues(channel):
    # Declare required queues
    queues = {'VLBIlogger': {'exchange': 'LOG', 'keys': ['']},
              'VLBIaux': {'keys': ['aux']},
              'VLBIcontrol': {'keys': ['control', 'ivscontrol']},
              'VLBIvgosdb': {'keys': ['V001', 'new-vgosdb']}
              }
    for name, info in queues.items():
        channel.queue_declare(name, durable=True)
        for key in info.get('keys', ['']):
            channel.queue_bind(exchange=info.get('exchange', 'VLBI'), queue=name, routing_key=key)


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser( description='Check for missing schedules' )

    parser.add_argument('user', help='RabbitMQ username')
    parser.add_argument('password', help='RabbitMQ password')

    args = parser.parse_args()

    host, port = 'localhost', 5672

    conn = connect(host, port, args.user, args.password)
    channel = conn.channel()

    create_exchanges(channel)
    create_queues(channel)

    conn.close()
from sshtunnel import SSHTunnelForwarder

class Tunnel:
    def __init__(self, config):
        self.server = None

        if config:
            self.server = SSHTunnelForwarder(config['host'], ssh_username=config['user'], ssh_pkey=config['rsa'],
                                             remote_bind_address=('127.0.0.1', config['remote']),
                                             local_bind_address=('127.0.0.1', config['local'])
                                             )

    def start(self):
        if self.server and not self.server.is_active:
            self.server.start()

    def close(self):
        if self.server and self.server.is_active:
            self.server.close()

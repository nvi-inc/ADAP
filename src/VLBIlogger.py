import sys
import os
import gzip
import logging
import logging.config

from datetime import datetime

from utils import app, readDICT
from rmq import Worker


# Custom filter use to format records
class ContextFilter(logging.Filter):
    @property
    def information(self):
        return self._information

    @information.setter
    def information(self, value):
        self._information = value

    def filter(self, record):
        for key, value in self._information.items():
            setattr(record, key, value)
        return True


# Functions needed to created file rotator with gzip compression
def rotator(source, destination):
    folder = os.path.dirname(destination)
    destination = os.path.join(folder, datetime.utcnow().strftime('%Y-%m-%d.%H%M%S.gz'))
    with open(source, "rb") as sf, open(destination, "wb") as df:
        df.write(gzip.compress(sf.read(), 9))
    os.remove(source)


# Logger using RabbitMQ queue to receive messages.
class Logger(Worker):

    def __init__(self):
        super().__init__()
        self.filter = self.logger = None
        self.exit_on_demand = None
        self.set_start_time('now')

    # Overwrite exit to make sure it is logged before exiting
    def exit(self):
        if self.exit_on_demand:
            sys.exit(0)
        return

    # Set special logger information for filtering information
    def begin(self):
        # Set logger dictionary
        logger_config = readDICT(os.path.expanduser(app.args.logger))
        logging.config.dictConfig(logger_config)
        self.exit_on_demand = logger_config.get('exit_on_demand', False)

        # Add custom levels
        [logging.addLevelName(code, name) for name, code in logger_config['levels'].items()]

        # Add the custom filter
        self.filter, self.logger = ContextFilter(), logging.getLogger('default')
        self.logger.addFilter(self.filter)

        # Add special functions for rotator
        for hd in self.logger.handlers:
            if isinstance(hd, logging.handlers.RotatingFileHandler):
                hd.rotator = rotator

        super().begin()

    # Process the message from queue
    def process_msg(self, ch, method, properties, body):
        try:
            if properties.headers and 'level' in properties.headers:
                level = logging.getLevelName(properties.headers['level'])
                if isinstance(level, int):
                    self.filter.information = properties.headers
                    self.logger.log(level, body.decode())
                else:  # BAD level
                    self.problem(f'BAD LEVEL {level} [{body.decode()}]')
        except:  # Bad message
            self.problem(f'UNEXPECTED ERROR {sys.exc_info()[0]} [{body.decode()}]')

    # Check if the END command is for itself
    def post_ack(self, ch, method, properties, body):
        if properties.headers['level'] == 'END':
            for key in ['app', 'server', 'pid']:
                if properties.headers.get(key, '') != self.header[key]:
                    return
            sys.exit(0)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Logger for ADAP software.' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-l', '--logger', help='logger config file', required=True)
    parser.add_argument('-q', '--queue', help='queue name', required=True)

    app.init(parser.parse_args())

    worker = Logger()
    worker.monit()

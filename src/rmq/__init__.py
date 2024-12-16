import os
import sys
import socket
import signal
import time
from copy import deepcopy
from datetime import datetime, timedelta

from urllib.parse import quote
from http import HTTPStatus
import pika
import requests
import json

from utils import app, readDICT, saveDICT


# Generic functions needed to work with server
# Connect to server using pika library
def connect(host, port, user, password):
    cred = pika.credentials.PlainCredentials(user, password)
    parameters = pika.ConnectionParameters(host=host, port=port, credentials=cred, heartbeat=0)
    return pika.BlockingConnection(parameters)


# Publish msg to exchange or queue
def publish(conn, exchange, route, message, headers=None, callback_queue=None, corr_id=None):
    if not conn or conn.is_closed:
        return

    properties = pika.BasicProperties(delivery_mode=2, headers=headers, reply_to=callback_queue, correlation_id=corr_id)
    publish_channel = conn.channel()
    publish_channel.basic_publish(exchange=exchange, routing_key=route, body=message, properties=properties)
    publish_channel.close()


class RMQclient:
    # init app with specific options
    def __init__(self):

        # Make sure it terminate elegantly after Ctr+C
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        # Create header for this application (used for vlba_logs and notifivations)
        self.header = {'app': os.path.basename(sys.argv[0]).split('.')[0],
                       'server': socket.gethostname().split('.')[0].split('-')[-1].upper(),
                       'pid': str(os.getpid())}

        # Initialize RabbitMQ connection parameters
        _, info = app.load_control_file(name=app.ControlFiles.RMQ, exit_on_error=True)
        self.server = app.make_object(info['Server'])
        self.exchanges = app.make_object(info['Exchanges'])
        self.problems = app.make_object(info['Problems'])
        self.attempts = 0
        # Keep consumer_tags
        self.consumer_tag = None
        self.conn = None

        # Decode user and password
        self.user, self.password = self.server.credentials.split(':')

        # Keep error message
        self._errors = []
        self.in_quiet = False

        self.timing_info = ('now', -1, -1)

    # Catch Ctrl+C
    def signal_handler(self, sig, frame):
        self.end('killed by manager')
        self.exit()

    # Information send when application terminate
    def end(self, msg):
        pass

    # Make sure everything is clean before exit
    def exit(self, msg=None, prnt=False):
        if prnt:
            print(f'failed {msg}' if msg else 'ended ok')
        sys.exit(0)

    # Send email
    def notify(self, msg, wait=True):
        title = '{hdr[app]} - {hdr[server]} '.format(hdr=self.header)

        # Read information on last error message
        path = os.path.join(os.path.expanduser(self.problems.folder), f'{self.header["app"]}.toml')
        if not (info := readDICT(path)):
            info = {'nbr': 0, 'last': datetime(1975, 1, 1), 'sent': datetime(1975, 1, 1), 'msg': ''}
        # Check if new error message
        if info['msg'] != msg:
            info['msg'], info['nbr'], info['sent'] = msg, 0, datetime(1975, 1, 1)
        else:
            info['nbr'] += 1

        # Check if message must be sent
        now = datetime.now()
        if not wait or (now - info['sent']).total_seconds() > self.problems.wait:
            app.notify(title, f'{str(msg)}\nrepeated {info["nbr"]} times in last {self.problems.wait} seconds')
            info['sent'] = now

        # Save information
        info['last'] = now
        saveDICT(path, info)

    # Add error to list
    def add_error(self, err, insert=False):
        self._errors.insert(0, str(err)) if insert else self._errors.append(str(err))
        for index, line in enumerate(err.splitlines()):
            self.logit('ERROR', f'{index:2d} - {line}')

    @property
    # Check if has errors
    def has_errors(self):
        return bool(self._errors)

    # Send error to watchdog if any error. Reset errors after sending
    def send_errors(self, wait=True):
        if self._errors:
            self.notify('\n'.join(self._errors), wait=wait)
            self._errors = []  # Clean error buffer

    # Request quiet time period from app and check if in quiet time period
    def is_quiet_time(self, log_info=True):
        start, end = app.get_quiet_time()
        if not (start and end):
            return False
        if start < datetime.now() < end:
            if not self.in_quiet and log_info:
                self.info('moving IN quiet time')
            self.in_quiet = True
        elif self.in_quiet:
            if log_info:
                self.info('moving OUT quiet time')
            self.in_quiet = False
        return self.in_quiet

    # Create RabbitMQ connection
    def connect(self):
        try:
            self.conn = connect(self.server.host, self.server.port, self.user, self.password)

            if self.conn.is_open:
                self.attempts = 0
            else:
                self.attempts += 1
                time.sleep(1)
        except Exception as err:
            self.attempts += 1
            time.sleep(1)

    def close(self):
        try:
            self.conn.close()
        except Exception as err:
            print('Broker close', str(err))

    # Publish message to root exchange
    def publish(self, routing_key, message, headers=None, callback_queue=None, corr_id=None, exchange=''):
        try:
            if not exchange:
                exchange = self.exchanges.log if routing_key == 'log' else self.exchanges.default
            publish(self.conn, exchange, routing_key, message, headers, callback_queue, corr_id)
        except Exception as err:
            self.problem(str(err))

    # Send problem message using a new connection
    def problem(self, msg):
        log_msg = '{hdr[app]},{hdr[pid]},{hdr[server]} - {msg}'.format(hdr=self.header, msg=msg)

        try:
            conn = connect(self.server.host, self.server.port, self.user, self.password)
            publish(conn, '', self.problems.queue, log_msg)
            conn.close()
        except Exception as err:
            msg += '\n{}'.format(str(err))
            self.notify(msg)

    # Fill record with required information before sending to 'log'
    def logit(self, level, msg):
        header = deepcopy(self.header)
        header['level'], header['time'] = level, datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4]
        self.publish('log', msg, header)

    # Information send when application start
    def begin(self):
        start, period, reset = [str(action) for action in self.timing_info]
        self.logit('BEGIN', f'({start},{period},{reset})')

    # Information send when application terminate
    def end(self, msg):
        self.logit('END', msg)

    # Critical message
    def critical(self, msg):
        self.logit('CRITICAL', msg)

    # Warning message
    def warning(self, msg):
        self.logit('WARNING', msg)

    # General information
    def info(self, msg):
        self.logit('INFO', msg)

    # An action is started
    def start(self, msg):
        self.logit('START', msg)

    # Action stopped
    def stop(self, msg):
        self.logit('STOP', msg)


# Class to access RABBITMQ API
class API(RMQclient):
    def __init__(self):
        super().__init__()

        self.base_url = f'http://{self.server.host}:{self.server.api:d}/api/'
        self.vhost = quote(self.server.vhost, safe='')

    # Do a PUT request
    def put(self, path, params):
        url = self.base_url + path
        headers = {'content-type': 'application/json'}
        data = json.dumps(params)

        rsp = requests.put(url, auth=(self.user, self.password), headers=headers, data=data)
        if not (ok := rsp.status_code == HTTPStatus.CREATED):
            self.add_error(f'{rsp.status_code} [{rsp.text}]')
        return ok, json.loads(rsp.text if rsp.status_code != HTTPStatus.NO_CONTENT else '{}')

    # Do a GET request
    def get(self, path):
        url = self.base_url + path
        headers = {'content-type': 'application/json'}
        try:
            rsp = requests.get(url, auth=(self.user, self.password), headers=headers)
            if not (ok := rsp.status_code == HTTPStatus.OK):
                self.add_error(f'{rsp.status_code} [{rsp.text}]')
            return ok, json.loads(rsp.text if rsp.status_code != HTTPStatus.NO_CONTENT else '{}')
        except Exception as err:
            self.add_error(str(err))
            return False, str(err)

    # Do a DELETE request
    def delete(self, path):
        url = self.base_url + path
        headers = {'content-type': 'application/json'}
        try:
            rsp = requests.delete(url, auth=(self.user, self.password), headers=headers)
            if not (ok := rsp.status_code == HTTPStatus.NO_CONTENT):
                self.add_error(f'{rsp.status_code} [{rsp.text}]')
            return ok, json.loads(rsp.text if rsp.status_code != HTTPStatus.NO_CONTENT else '{}')
        except Exception as err:
            self.add_error(str(err))
            return False, str(err)

    # Get an item
    def get_items(self, name):
        ok, data = self.get(name)
        return data if ok else []


# Basic class for all DF server
class Worker(RMQclient):
    # init app with specific options
    def __init__(self):
        super().__init__()

        # Application attached to this queue
        self.listen_queue = app.args.queue
        # Maximum timeout without receiving information
        self.timeout, self.initial_timeout, self.constant_timeout, self.timeout_id = 21600, 0, False, None
        # Flag to reset time-out when message have been processed
        self.reset_timeout, self.attempts, self.exclusive_queue = True, 0, False

    # Jump to next interval
    @staticmethod
    def get_next_start_time(start, interval):
        return start + timedelta(seconds=((int((datetime.utcnow() - start).total_seconds() / interval) + 1) * interval))

    # Set start time
    def set_start_time(self, start, timeout=21600, reset_timeout=True):
        self.timing_info = (start, timeout, reset_timeout)
        self.timeout, self.initial_timeout, self.reset_timeout = timeout, 0, reset_timeout
        if start != 'now':
            hour, minute = start.split(':')
            start_time = datetime.utcnow().replace(minute=int(minute)) if hour == '**'\
                else datetime.utcnow().replace(hour=int(hour), minute=int(minute), second=0, microsecond=0)
            if start_time < datetime.utcnow():
                start_time = self.get_next_start_time(start_time, timeout)
            self.initial_timeout = (start_time - datetime.utcnow()).total_seconds()
            self.constant_timeout = True

    # Process event from connection
    def process_events(self, info='no info'):
        self.info(f'process_events: {info}')
        try:
            self.conn.process_data_events(0)
        except Exception as err:
            self.exit('PROCESS_EVENTS', str(err))

    # Use connection to sleep 1 second.
    def sleep_1sec(self):
        if self.conn and self.conn.is_open:
            self.conn.sleep(1)
        else:
            time.sleep(1)

    # Start monitoring RabbitMQ queue.
    def monit(self):
        self.connect()
        # Send beginning information to logger
        self.begin()
        # start queue consumption
        self.monitor()

    # Test the connection is open
    def test_connection(self):
        for i in range(10):
            if self.conn.is_open:
                return True
            time.sleep(1)
        return False

    # Set new value for start time
    def reset_start_time(self):
        start_time, timeout, reset_timeout = self.timing_info
        self.set_start_time(start_time, timeout, reset_timeout)

    # Reconnect
    def reconnect(self, msg):
        if self.attempts % 10 == 0:
            self.notify(f'{self.header["pid"]} tried to reconnect {self.attempts+1:d} {self.consumer_tag}')
        if self.attempts > 100:
            self.notify(f'Too many connect retries.\n{msg}', wait=False)
            self.exit()
        # Sleep few seconds before trying to reconnect
        time.sleep(5)

        self.reset_start_time()
        self.connect()
        self.warning(f'reconnected after {self.attempts:d} tries.\n{msg}')
        self.monitor()

    # Basic function called on time out.
    def process_timeout(self):
        self.info('still alive')

    # Create exclusive queue
    def create_exclusive_queue(self, channel):
        if self.exclusive_queue:
            try:
                channel.queue_declare(queue=self.listen_queue, exclusive=True)
            except Exception as err:
                self.notify(f'Exclusive queue exists! {str(err)}', wait=False)
                self.end('Exclusive queue exists')
                self.exit()

    # Callback function for timeout signal
    def on_timeout(self):
        try:
            last = datetime.now()
            self.process_timeout()
            if self.constant_timeout:
                dt = (datetime.now() - last).total_seconds()
                wait_time = self.timeout - dt if dt < self.timeout else (int(dt/self.timeout)+1) * self.timeout - dt
            else:
                wait_time = self.timeout

            self.timeout_id = self.conn.call_later(wait_time, self.on_timeout)
        except Exception as e:
            self.warning(f'ON TIMEOUT {str(e)}')

    # Clean consumers. Not using heartbeat so must clean old connection using customer tag
    def clean_consumer(self):
        if self.consumer_tag: # Use API to delete old connection
            api = API(self.args)
            for consumer in api.get_items('consumers'):
                if consumer['consumer_tag'] == self.consumer_tag:
                    connection = consumer.get('connection_name', '')
                    if connection:
                        ok, _ = api.delete('connections/{}'.format(connection))
                        self.notify('{} Consumer {} has{} been deleted'.format(self.listen_queue, self.consumer_tag, '' if ok else ' NOT'))
            self.consumer_tag = None

    # Keep consumer tag if need to delete
    def set_consumer(self, channel):
        if self.consumer_tag:
            self.clean_consumer()
        self.consumer_tag = channel.consumer_tags[0] if channel.consumer_tags else None

    # Initiate queue monitoring with time out
    def monitor(self):
        try:
            self.clean_consumer()
            self.timeout_id = self.conn.call_later(self.initial_timeout, self.on_timeout)
            self.monitor_channel = self.conn.channel()
            self.monitor_channel.basic_qos(prefetch_count=1)
            self.create_exclusive_queue(self.monitor_channel)
            self.monitor_channel.basic_consume(self.listen_queue, self.msg_received)
            self.set_consumer(self.monitor_channel)
            self.monitor_channel.start_consuming()
        except pika.exceptions.ConnectionClosedByBroker:
            self.notify('Connection closed by server', wait=False)
            self.exit()
        except pika.exceptions.ConnectionClosed:
            self.reconnect('Connection closed)')
        except pika.exceptions.AMQPError as e:
            self.reconnect('AMQError [{}]'.format(str(e)))

    # Process received message. Overriden by derived classes
    def process_msg(self, ch, method, properties, body):
        pass

    # function call after message has been ack. Overriden by some derived classes
    def post_ack(self, ch, method, properties, body):
        pass

    # Call back function when new message received
    def msg_received(self, ch, method, properties, body):
        if self.reset_timeout:
            self.conn.remove_timeout(self.timeout_id)
        if body.decode().strip().lower() == 'stop':  # close connection on receiving stop message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.end('stopped by ADAP manager')
            self.exit()
        else: # Process message
            self.process_msg(ch, method, properties, body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.post_ack(ch, method, properties, body)
            if self.reset_timeout:
                self.timeout_id = self.conn.call_later(self.timeout, self.on_timeout)





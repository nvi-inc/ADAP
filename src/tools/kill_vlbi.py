import time
from collections import defaultdict, namedtuple
from pathlib import Path

import psutil
from psutil import Process, process_iter

from utils import app
from rmq import API

Addr = namedtuple('addr', 'ip port')
class VLBIKiller(API):

    def __init__(self, app_name):
        super().__init__()

        self.connect()

        self.queues, self.processes, self.ports = set(), {}, {}
        self.get_rmq_info(Path(app_name).stem)

    def stop_consumers(self):
        nbr_prc = 0
        for queue in self.queues:
            ok, info = self.get(f'queues/{self.vhost}/{queue}')
            if ok:  # Send stop message for each consumer
                nbr_prc = max(nbr_prc, (nbr := info.get('consumers', 0)))
                for _ in range(nbr):
                    self.publish(queue, 'stop', exchange='')

        time.sleep(0.1 * nbr_prc)
        waiting = []
        for pid in self.ports.values():
            if not psutil.pid_exists(pid):
                print(f"Process {self.processes[pid]} {pid} was stopped")
            else:
                waiting.append(pid)
        if not waiting:
            return
        print(f'Waiting for {len(waiting)} processes to complete.')
        ok, consumers = self.get('consumers')
        if ok:
            for consumer in consumers:
                if (queue := consumer['queue']['name']) in self.queues:
                    for _ in range(100):
                        # Check if queue has one unacknowledged message.
                        ok, info = self.get(f'queues/{self.vhost}/{queue}')
                        if not ok:  # No queue, all consumers were stopped
                            pid = self.ports[consumer['channel_details']['peer_port']]
                            print(f"Process {self.processes[pid]} {pid} was stopped")
                            break
                        if info.get('messages_ready', 0) == 0 and info.get('messages_unacknowledged', 0) == 0:
                            self.kill_process(consumer['channel_details']['peer_port'])
                            break
                        else:  # Wait that process finishes action before killing it.
                            time.sleep(0.1)
                    else:
                        self.kill_process(consumer['channel_details']['peer_port'])
        # Kill any application that has not been stopped
        time.sleep(0.1)
        for port, pid in self.ports.items():
            if pid in waiting and psutil.pid_exists(pid):
                self.kill_process(port)

    def kill_process(self, port):
        pid = self.ports[port]
        try:
            Process(pid).kill()
            print(f"Process {self.processes[pid]} {pid} killed!")
        except Exception as err:
            print(f"Failed to kill process {self.processes[pid]} {pid}! [{str(err)}]")

    # Check if RabbitMQ is alive
    def alive(self):
        code, ans = self.get(f'aliveness-test/{self.vhost}/')
        if not code:
            self.add_error(f'aliveness-test failed! {ans}')
        return code

    def get_rmq_info(self, app_name):
        for prc in process_iter(attrs=['pid', 'name', 'cmdline']):
            if prc.info['name'] == 'python' and '-q' in (cmds := prc.info['cmdline']):
                if cmds[1].startswith('/sgpvlbi/progs/adap') and app_name in ((name := Path(cmds[1]).stem), 'all'):
                    self.queues.add(cmds[cmds.index('-q') + 1].strip())
                    self.processes[prc.pid] = name

        for prc in process_iter(attrs=['pid', 'name', 'cmdline']):
            if prc.pid in self.processes:
                try:
                    for conn in prc.net_connections(kind='tcp'):
                        if conn.laddr and conn.raddr:
                            laddr, raddr = Addr(*conn.laddr), Addr(*conn.raddr)
                            if raddr.port == self.server.port:
                                self.ports[laddr.port] = prc.pid
                except psutil.AccessDenied:
                    pass


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Logger for ADAP software.' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('app', help='application name')

    args = app.init(parser.parse_args())
    if (killer := VLBIKiller(args.app)).alive():
        killer.stop_consumers()

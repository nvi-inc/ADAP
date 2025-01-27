import os
import time
import inspect
import psutil

from collections import namedtuple
from importlib import import_module
from pathlib import Path
from psutil import Process, process_iter
from subprocess import Popen, PIPE
from typing import List

from utils import app
from rmq import API


Addr = namedtuple('addr', 'ip port')
class VLBIKiller(API):

    def __init__(self, app_names):
        if not (app_dir := os.environ.get('APP_DIR')):
            raise Exception('APP_DIR not defined in environment')
        self.app_dir = str(Path(app_dir, 'src'))

        super().__init__()

        self.connect()

        self.apps, self.queues, self.processes, self.ports = [], set(), {}, {}

        self.get_running_apps(app_names)

        self.get_rmq_info(Path(app_names).stem)

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

    def get_running_apps(self, app_names):
        if app_names[0].lower() == 'all':
            self.apps = []
            for prc in process_iter(attrs=['pid', 'name', 'cmdline']):
                if prc.info['name'] == 'python' and '-q' in (cmds := prc.info['cmdline']):
                    if cmds[1].startswith(self.app_dir):
                        self.apps.append(Path(cmds[1]).stem)
        else:
            self.apps = [Path(name).stem for name in app_names]

    def get_rmq_info(self, app_names):
        for prc in process_iter(attrs=['pid', 'name', 'cmdline']):
            if prc.info['name'] == 'python' and '-q' in (cmds := prc.info['cmdline']):
                if cmds[1].startswith(self.app_dir) and (name := Path(cmds[1]).stem) in app_names:
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

    def list(self):
        print(f"Applications to kill: {' '.join(self.apps)}")

    def grep(self, cmd: str):
        try:
            full_cmd = f'grep -rE --include \*.py \"{cmd}\" {self.app_dir}'
            #print(full_cmd)
            st_out, _ = Popen(full_cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE).communicate()
            #print(st_out.decode('utf-8'))
            return st_out.decode('utf-8').splitlines()
        except:
            return []


    @staticmethod
    def get_module_items(path):
        items = set()
        module_name = f'{path.parent.name}.{path.stem}'
        #print('module', module_name)
        the_module = import_module(module_name)
        for name, obj in the_module.__dict__.items():
            if not (obj_module := inspect.getmodule(obj)) or obj_module.__name__ == module_name:
                if inspect.isclass(obj) or inspect.isfunction(obj) or not name.startswith(('_', 'last_mod_time')):
                    items.add(name)

        return items


    def find_files(self, path):
        package, name, cmds = path.parent.name, path.stem, []
        if path.stem == '__init__':
            cmds = [f'import {package}\\s?$', f'from {package} import.*({"|".join(self.get_module_items(path))})']
        elif path.stem == '__main__':
            pass
        else:
            cmds = [f'from {package} import.*{name}', f'from {package}\.{name} import']

        files = [Path(line.split(':')[0]) for cmd in cmds for line in self.grep(cmd)]
        return sorted(set(files))


    def find_apps(self, names):
        for name in names:
            path = Path()
        if not (path := Path(self.app_dir, package, f'{name}.py')).is_file():
            return []

        applications, processed, to_check = set(), set(), []
        while True:
            processed.add(path)

            for file in self.find_files(path):
                if file.name.startswith('VLBI'):
                    applications.add(file)
                elif str(file.parent) != self.app_dir and file not in processed:
                    to_check.append(file)
            if not to_check:
                break
            path = to_check.pop(0)

        return [path.stem for path in applications]


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Logger for ADAP software.' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-m', '--modified', help='python file', action='store_true', required=False)
    parser.add_argument('-l', '--list', help='list active app', action='store_true', required=False)
    parser.add_argument('names', help='application name', nargs='+')

    args = app.init(parser.parse_args())

    print(args.app)
    if (killer := VLBIKiller(['all'] if args.modified else args.names)).alive():
        if args.modified:
            killer.find_apps(args.names)
        killer.stop_consumers()

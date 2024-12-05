import os
import subprocess
import traceback
from datetime import datetime
from psutil import Process, process_iter
from tempfile import NamedTemporaryFile
import glob
import requests
from bs4 import BeautifulSoup


from utils import app
from utils.files import remove
from utils.servers import load_servers, get_server, DATACENTER, SERVER
from rmq import API


class Watchdog(API):

    def __init__(self):
        super().__init__()

        self.connect()

        self.wd_config = app.load_control_file(name=app.ControlFiles.Watchdog)[-1]
        load_servers(DATACENTER)

    def whoami(self):
        ok, ans = self.get('whoami')
        self.info(f'whoami : {ans if ok else "failed"}')

    def vhosts(self):
        ok, vhosts = self.get('vhosts')
        if ok:
            for vhost in vhosts:
                self.info(f'vhost: {vhost}')

    def queue(self, name):
        return self.get(f'queues/{self.vhost}/{name}')

    def queues(self):
        return self.get('queues')

    def channels(self):
        ok, channels = self.get('channels')
        if ok:
            for channel in channels:
                print(f'channel: {channel}')
        else:
            print()

    def get_consumers(self):
        ok, consumers = self.get('consumers')
        if ok:
            for consumer in consumers:
                print(consumer)

    # Check if RabbitMQ is alive
    def alive(self):
        code, ans = self.get(f'aliveness-test/{self.vhost}/')
        if not code:
            self.add_error(f'aliveness-test failed! {ans}')
        return code

    def cluster(self):
        return self.get('cluster-name')

    # Make a queue if is does not exist
    def make_queue(self, name):
        path = 'queues/{vhost}/{name}'.format(vhost=self.vhost, name=name)
        params = {"auto_delete":False,"durable":True,"arguments":{}}
        return self.put(path, params)

    # Restart a script
    @staticmethod
    def restart(script):
        return app.exec_and_wait(script)[-1]

    def get_last_problem(self, script):
        try:
            path = os.path.join(self.problems['folder'], os.path.basename(script))
            if os.path.exists(path):
                with open(path) as log:
                    return '{}\n{}'.format(path, '\n'.join(log.readlines()))
        except:
            pass
        return ''

    def test_scanner(self, key_word):
        def get_last_scan(grep, filename):
            cmd = f"{grep} 'STOP     VLBIscanner' {filename} | grep \"{key_word}\" | tail -1"
            st_out, st_err = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                              stderr=subprocess.PIPE).communicate()

            if st_err:
                return False, st_err.decode('utf-8')
            return True, st_out.decode('utf-8')

        logfile = app.load_control_file(name='logger.toml')[-1]['handlers']['file']['filename']
        ok, line = get_last_scan('grep', logfile)
        if not ok or not line.strip():
            folder = os.path.dirname(logfile)
            gz = sorted([file for file in os.listdir(folder) if file.endswith('.gz')])
            ok, line = get_last_scan('zgrep', os.path.join(folder, gz[-1])) if gz else (False, 'no gz')
        if not ok or not line.strip():
            self.add_error(f'{key_word} not found in {logfile} - {ok} {line}')
        else:
            try:
                last = datetime.strptime(line[:19], '%Y-%m-%d %H:%M:%S')
                delta = datetime.utcnow() - last
                if delta.total_seconds() > 18000:
                    self.add_error(f'{key_word} last scan too old {delta}\n{line}\n{last}')
            except Exception as e:
                self.add_error(f'{key_word} last scan {line} has error {str(e)}')

    # Test if files are been waiting for to long in failed_upload folder
    def check_failed_upload(self):
        folder = app.Applications.VLBI['failed_upload']
        self.info(f'check {folder} for forgotten files')
        now = datetime.now()
        for name in os.listdir(folder):
            if os.path.isfile(path := os.path.join(folder, name)):
                if (dt := int((now - datetime.fromtimestamp(os.path.getmtime(path))).total_seconds() / 60)) > 15:
                    self.add_error(f'{name} has been waiting over {dt} minutes for upload')

    # Test that all queues have one consumer processing messages
    def test_queues(self):
        self.info('check VLBI queues')
        for (action, restart_it) in [('monit', True), ('test', False)]:
            for name, script in self.wd_config['Queues'][action].items():
                script = script if os.path.exists(script) else os.path.join(os.environ['APP_DIR'], script)
                ok, info = self.queue(name)
                if not ok or info['consumers'] == 0:  # No consumer
                    self.kill_queue_processes(name)  # Kill running processes not connected to RabbitMQ
                    problem = self.get_last_problem(script)
                    if restart_it:
                        if err := self.restart(script):
                            self.add_error(f'Watchdog found that {name} has no worker and could not re-start {script}\n{err}')
                        else:
                            self.add_error(f'Watchdog found that {name} had no worker and re-started {script}')
                    else:
                        self.add_error(f'Watchdog found that {name} has no consumers')
                    self.add_error(problem)
                else:
                    if info['consumers'] > 1:
                        self.add_error(f'queue {name} has {info["consumers"]} consumers')
                    self.check_queue_processes(name)
                    if info['messages'] > 10: # Should not have too many messages in queue
                        self.add_error(f'queue {name} has {info["messages"]} messages')

    # Read summary file to extract updated time

    # Test massloading files
    def check_massloading(self):
        def get_updated_massloading(path):
            if os.path.exists(path):
                with open(path) as sum:
                    for line in sum:
                        if line.startswith('LAST_UPDATE:'):
                            return datetime.strptime(line.split()[1].strip(), '%Y.%m.%d-%H:%M:%S')
            return None

        self.info('check massloading')
        config = app.load_control_file(name=app.ControlFiles.MassLoading)[-1]
        server_name, summary = config['server'], config['summary']
        with get_server(SERVER, server_name) as server:
            for model, info in config['Model'].items():
                folder = info['folder']
                tpath = NamedTemporaryFile(suffix='_'+summary, delete=False).name
                rpath = os.path.join(server.root, model, summary)
                try:
                    if not server.download(rpath, tpath):
                        self.add_error(f'Could not download summary for {model}\n{server.errors}')
                    elif last := get_updated_massloading(tpath):
                        if current := get_updated_massloading(os.path.join(folder, summary)):
                            if (current - last).total_seconds() < 3600:
                                self.info(f'{model} {summary} {last} {current} OK')
                            else:
                                self.info(msg := f'{model} {summary} not current [{last}:{current}')
                                self.add_error(msg)
                        else:
                            self.add_error(f'could not read summary for {model} on local server')
                    else:
                        self.add_error(f'could not read summary for {model} on {server_name}')
                except Exception as err:
                    self.add_error(f'{model} failed {str(err)}')
                remove(tpath)

    # Get the last vmf file that was uploaded and check if too old
    def check_last_vmf_file(self, year=None):
        self.info('check vmf files')
        folder, now = app.VLBIfolders.vmf, datetime.now().date()
        year = year if year else int(now.strftime('%Y'))
        try:
            if files := sorted(glob.glob(os.path.join(folder, str(year), '*.vmf3_r'))):
                if (days := (now - datetime.strptime(os.path.basename(files[-1]), '%Y%j.vmf3_r').date()).days) > 2:
                    self.add_error(f'No new vmf files for {days}')
            else:
                self.check_last_vmf_file(year - 1)
        except Exception as err:
            self.add_error(f'check vmf failed {str(err)}')

    # Check that queue has
    def kill_queue_processes(self, name):
        is_queue = lambda lst: True if name in lst and lst[max(lst.index(name)-1, 0)] == '-q' else False
        for pid in [prc.info['pid'] for prc in process_iter(attrs=['pid', 'name', 'cmdline'])
            if prc.info['name'] == 'python' and is_queue(prc.info['cmdline'])]:
            try:
                Process(pid).kill()
                killed = f'Successfully killed process for queue {name}'
            except Exception as err:
                killed = f'Failed trying to kill process for queue {name} {pid}. [{str(err)}]'
            self.add_error(killed)

    # Check that queue has
    def check_queue_processes(self, name):
        is_queue = lambda lst: True if name in lst and lst[max(lst.index(name)-1, 0)] == '-q' else False
        if not (pids := [prc.info['pid'] for prc in process_iter(attrs=['pid', 'name', 'cmdline'])
                         if prc.info['name'] == 'python' and is_queue(prc.info['cmdline'])]):
            self.add_error(f'no processes for queue {name}')
        elif len(pids) > 1:
            self.add_error(f'too many processes for {name} {pids}')

    # Check if the submitted EOP are not too old
    def check_apriori_eop(self):
        def get_eop_file(center):
            for try_id in range(5):
                with get_server(DATACENTER, center) as server:
                    rpath = os.path.join(server.root, path)
                    if server.download(rpath, tpath)[0]:
                        return True
            return False

        self.info('check apriori eop')
        options = self.wd_config['AprioriEOP']
        max_delay = options['max_delay']
        for name, path in options['files'].items():
            tpath = NamedTemporaryFile(prefix=name+'.', delete=False).name
            # Download file
            if not get_eop_file(options['server']):
                self.add_error(f'could not download {name}')
            else:
                # Read file to find created time
                with open(tpath) as file:
                    for line in file:
                        if line.startswith('# File was created on'):
                            try:
                                created = datetime.strptime(line.strip().split()[-1], '%Y.%m.%d-%H:%M:%S')
                                age = (datetime.now() - created).total_seconds() / 3600
                                if age > max_delay:
                                    self.add_error(f'{name} is {age} hours old')
                            except Exception as err:
                                self.add_error(f'{name} {line} problem [{str(err)}]')
                            break
                    else:
                        self.add_error(f'Not able to check created time for {name}')
            if os.path.exists(tpath):
                os.remove(tpath)

    # Check when the last scanner has recorded information
    def check_scanners(self):
        for key_word in self.wd_config['Scanners']['keys']:
            self.test_scanner(key_word)

    # Check time on IVSCC sessions page
    def check_ivscc(self):
        url = self.wd_config['Web']['url']
        session = requests.Session()
        session.headers = {'UserAgent': 'Mozilla/5.0 (X11; Linux; rv:74.0) Gecko/20100101 Firefox/74.0'}

        if rsp := session.get(url):
            try:
                html = BeautifulSoup(rsp.text, 'html.parser')
                updated = datetime.strptime(html.select('time')[0].get('datetime'), '%Y-%m-%d %H:%M UTC')
                if (datetime.utcnow() - updated).total_seconds() > 7200:
                    self.add_error(f'IVSCC sessions page has not been updated since {updated} UT')
            except Exception as exc:
                self.add_error(f'Could not check IVSCC sessions page [{str(err)}]')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Watchdog for ADAP software.')
    parser.add_argument('-c', '--config', help='vlbi config file', required=True)

    app.init(parser.parse_args())

    watchdog = Watchdog()
    try:
        if watchdog.alive():
            watchdog.test_queues()

        watchdog.check_failed_upload()
        watchdog.check_last_vmf_file()
        watchdog.check_apriori_eop()
        watchdog.check_massloading()
        watchdog.check_scanners()
        watchdog.check_ivscc()
    except Exception as err:
        watchdog.add_error(str(err))
        watchdog.add_error(traceback.format_exc())
    watchdog.send_errors(wait=False)





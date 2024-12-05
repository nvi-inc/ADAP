from paramiko import SSHClient
from scp import SCPClient
import os

class REMOTEserver:
    def __init__(self, server):
        self.server = server
        self.errors = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def is_ready(self):
        return not bool(self.errors)

    def open(self):
        if self.is_ready:
            try:
                ssh = SSHClient()
                ssh.load_system_host_keys()
                ssh.connect(self.server)
                self.scp = SCPClient(ssh.get_transport())
            except Exception as err:
                self.errors = str(err)

    def close(self):
        if self.is_ready:
            self.scp.close()

    def put(self, path):
        if self.is_ready and os.path.exists(path):
            try:
                if os.path.isdir(path):
                    rpath = os.path.dirname(path)
                    self.scp.put(path, rpath, recursive=True, preserve_times=True)
                else:
                    self.scp.put(path, path, preserve_times=True)
                print(path, 'Ok')
                return True
            except Exception as err:
                print('ERROR', path, str(err))
        else:
            print(path, self.is_ready, os.path.exists(path))
        return False
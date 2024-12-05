import paramiko
import os

# Class to update esdwebdev server

class ESDWebDev:
    def __init__(self, server):
        self.host, self.user, self.root = server['host'], server['user'], server['root']

        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.load_system_host_keys(os.path.expanduser('~/.ssh/id_rsa'))

        self.sftp = None

        self._errors, self.updated = [], []

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def open(self):
        if not self.sftp:
            try:
                self.client.connect(self.host, username=self.user, banner_timeout=60)
                self.sftp = self.client.open_sftp()
            except Exception as err:
                self.sftp = None
                self.add_error(str(err))

    def close(self):
        if self.sftp:
            self.sftp.close()
            self.client.close()
            self.sftp = None

    def add_error(self, msg):
        self._errors.append(msg)

    # Test if has errors
    @property
    def has_errors(self):
        return bool(self._errors)

    # Return list of errors (1 per line)
    @property
    def errors(self):
        return '\n'.join(self._errors)

    # Test if file exists
    def exists(self, path):
        try:
            self.sftp.stat(path)
            return True
        except FileNotFoundError:
            return False

    # Make directory on remote host
    def make_dirs(self, dirname):
        if not self.exists(dirname):
            self.make_dirs(os.path.dirname(dirname))
            try:
                self.sftp.mkdir(dirname)
            except:
                self.add_error(f'Could not create {dirname}')

    # Copy local file to remote server
    def copy(self, lpath, rpath):
        self.make_dirs(os.path.dirname(rpath))
        try:
            self.sftp.put(lpath, rpath, confirm=True)
        except:
            self.add_error('Could not copy file')

    # Save data on remote server
    def save(self, folder, data):
        if data:
            path = os.path.join(self.root, folder, 'index.html')
            if not self.exists(path):
                self.make_dirs(os.path.dirname(path))
            try:
                with self.sftp.open(path, 'w') as f:
                    f.write(data)
                self.updated.append(path)
            except Exception as err:
                self.add_error(f'{path} failed! {str(err)}')


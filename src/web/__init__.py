import traceback

import paramiko
import os


# Class to update esdwebdev server
class ESDWebDev:
    def __init__(self, server):
        self.host, self.port, self.root = server['host'], server.get('port', None), server['root']
        self.user = server['user']
        self.files = server['files']
        self.key = paramiko.RSAKey.from_private_key_file(server.get('id_rsa', '~/.ssh/id_rsa'))

        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
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
                self.client.connect(self.host, self.port, username=self.user, pkey=self.key, allow_agent=True,
                                    banner_timeout=60)
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
    def make_dirs(self, dir_name):
        if not self.exists(dir_name):
            self.make_dirs(os.path.dirname(dir_name))
            try:
                self.sftp.mkdir(dir_name)
            except:
                print('make_dirs', dir_name)
                self.add_error(f'Could not create {dir_name}')

    # Copy local file to remote server
    def copy(self, local_path, remote_path):
        self.make_dirs(os.path.dirname(remote_path))
        try:
            self.sftp.put(local_path, remote_path, confirm=True)
        except IOError as exc:
            self.add_error(f'Could not copy file ({str(exc)})')

    # Save data on remote server
    def save(self, folder, data, name='index.html'):
        if data:
            path = os.path.join(self.root, folder, name)
            if not self.exists(path):
                self.make_dirs(os.path.dirname(path))
            try:
                with self.sftp.open(path, 'w') as f:
                    f.write(data)
                self.updated.append(path)
            except IOError as exc:
                print('IO error', str(err))
                self.add_error(f'{path} failed! {str(exc)}')


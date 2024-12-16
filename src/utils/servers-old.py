import os
import ssl
import re
import time
from datetime import datetime, timedelta

from ftplib import FTP_TLS, FTP
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from bs4 import BeautifulSoup
import pytz
import hashlib
import requests
import pycurl
from io import BytesIO

from utils import app

# Define globals variables
configurations = {}
DATACENTER = 'DataCenter'
CORRELATOR = 'Correlator'
SERVER = 'Server'

categories = [DATACENTER, CORRELATOR, SERVER]
last_mod_time = None


# HTTPAdapter to lower cypher level so that some https servers could be accessed
class TLSAdapter(HTTPAdapter):

    def init_poolmanager(self, connections, maxsize, block=False):
        """Create and initialize the urllib3 PoolManager."""
        ctx = ssl.create_default_context()
        ctx.set_ciphers('DEFAULT@SECLEVEL=1')
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_version=ssl.PROTOCOL_TLSv1,
            ssl_context=ctx)


class FTPserver:
    T0 = pytz.UTC.localize(datetime(1975, 1, 1))  # Time before VLBI but not 0 when computing timestamp

    # Initial server using configuration
    def __init__(self, configuration):

        # Initialize some variables
        self._errors, self.connected = [], False
        self._warnings = []

        # Parameters specific to server
        self.protocol = configuration.get('protocol', 'ftp')
        self.tz = pytz.timezone(configuration.get('timezone', 'UTC'))  # Server timezone
        self.url = configuration.get('url', '')
        self.root = configuration.get('root', '/pub/vlbi')
        self.scan = configuration.get('scan', '')
        self.file_name = configuration.get('file_name', '')
        self.code = configuration.get('name', self.url)
        self.script = configuration.get('script', '')
        # Get name of upload function for this server
        upload = configuration.get('upload', 'no_upload')
        self.upload = getattr(self, upload if hasattr(self, upload) else 'no_upload')
        # variables to keep track of last folder read
        self.last_dir, self.last_folders, self.last_files = None, [], []

    # Called when using 'with IVScenter() as'
    def __enter__(self):
        self.connect()
        return self

    # Called when ending 'with IVScenter() as'
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # Add error message to _errors list
    def add_error(self, msg, is_error=True):
        self._errors.append(msg) if is_error else self._warnings.append(msg)

    # Return string of error messages and reset
    @property
    def errors(self):
        txt = '\n'.join(self._errors)
        self._errors = []
        return txt

    @property
    def warnings(self):
        lines = [line for line in self._warnings]
        self._warnings = []
        return lines

    # Test if connected
    @property
    def is_connected(self):
        return self.connected

    def try2connect(self):
        try:
            if self.protocol == 'sftp':
                self.host = FTP_TLS(host=self.url, timeout=5)
                self.host.login()
                self.host.prot_p()
            else:
                self.host = FTP(host=self.url, timeout=5)
                self.host.login()
            # Set passive mode
            self.host.set_pasv(True)
            self.connected = True
            return True
        except Exception as err:
            self.add_error(f'could not connect to {self.url} [{str(err)}]')
            return False

    # Connect to server
    def connect(self):
        if not self.url:
            self.add_error(f'url is null')
            return

        for iteration in range(3):
            if self.try2connect():
                return
            self.add_error(f'connect to {self.url} iter {iteration}', is_error=False)
            time.sleep(5)

    # Close connection
    def close(self):
        try:
            self.host.close()
        except:
            pass
        self.connected = False

    # Upload file to ivs center (This is specific to each server)
    def no_upload(self, lst, testing=False):
        self.add_error(f'cannot upload to {self.code}')
        return 0

    # List files in directory with their timestamp
    def listdir(self, folder, reset=True):
        # Check if reading the same folder
        if not reset and folder == self.last_dir:
            return self.last_folders, self.last_files

        self.last_dir, self.last_folders, self.last_files = folder, [], []
        lines = []
        try:
            self.host.dir(folder, lines.append)
        except Exception as err:
            self.add_error(str(err))

        for line in lines:
            info = line.split()
            if info[0].startswith('d'):
                self.last_folders.append(info[-1])
            else:
                self.last_files.append((info[-1], self.decode_ftptime(' '.join(info[-4:-1]))))
        return self.last_folders, self.last_files

    # Decode time stamp
    def decode_ftptime(self, text):
        try:
            now = self.tz.localize(datetime.now() + timedelta(seconds=120))  # In case servers are not sync
            year = int(now.strftime('%Y'))
            s2t = lambda y: datetime.strptime(f'{y} {text}', '%Y %b %d %H:%M')

            if (time_value := self.tz.localize(s2t(year))) > now:
                time_value = self.tz.localize(s2t(year-1))
        except Exception as err:
            time_value = self.decode_old_ftptime(text)
        # Change to UTC and timestamp
        return int(time_value.astimezone(pytz.UTC).timestamp())

    # Try another format for decoding time
    def decode_old_ftptime(self, text):
        try:
            return self.tz.localize(datetime.strptime(text, '%b %d %Y'))
        except:
            return self.T0  # Could not decode time

    # Detect if file exist and provide timestamp
    def get_file_info(self, path, reset=True):
        if not self.is_connected:
            return False, 0
        _, files = self.listdir(os.path.dirname(path), reset)
        timestamp = dict(files).get(os.path.basename(path), 0)
        return timestamp > 0, timestamp

    # Get timestamp for specific file
    def get_timestamp(self, path, reset=True):
        return self.get_file_info(path, reset)[1]

    # Check if file exists
    def exists(self, path, reset=True):
        return self.get_file_info(path, reset)[0]

    # Download file and compute MD5 check sum
    def download(self, rpath, lpath):
        if not self.is_connected:
            return False, 'connection closed'
        try:
            with open(lpath, 'wb') as f:
                md5 = hashlib.md5()
                def process(chunk):
                    md5.update(chunk)
                    f.write(chunk)
                self.host.retrbinary(f'RETR {rpath}', process)

                return True, md5.hexdigest()
        except Exception as err:
            self.add_error('download {} failed : [{}]'.format(rpath, str(err)))
            return False, self.errors

    # Walk function using lisdir
    def _walk(self, directory):
        subdirs, files = self.listdir(directory)

        yield(directory, files)

        for subdir in subdirs:
            for x in self._walk(os.path.join(directory, subdir)):
                yield x

    # Walk through all directories under top and extract all files with their timestamp
    def walk(self, top, reject=[]):
        if self.is_connected:
            for root, files in self._walk(top):
                for filename, timestamp in files:
                    if filename not in reject:
                        path = os.path.join(root, filename)
                        yield (filename, path, timestamp)

    # Specific upload function for cddis
    def upload_cddis(self, lst, testing=False):

        uploader = requests.Session()
        uploader.mount('https://', TLSAdapter(pool_connections=100, pool_maxsize=100))

        # Login to cddis to get cookies and insert in cookie jar
        rsp = uploader.get(self.script + 'login')
        if rsp.status_code != 200 or 'Welcome' not in rsp.text:
            self.add_error(rsp.text)
            return []
        jar = requests.cookies.RequestsCookieJar()
        for r in rsp.history:
            jar.update(r.cookies)

        # Form files data
        files = [('fileType', (None, 'MISC')), ('fileContentType', (None, 'MISC'))] if testing else [('fileType', (None, 'VLBI'))]
        files.extend([('file[]', (os.path.basename(path), open(path, 'rb'))) for path in lst if os.path.exists(path)])
        if len(files) > 1:
            rsp = uploader.post(self.script + 'upload/', cookies=jar, files=files)
            return [line.split(':')[1].strip() for line in rsp.text.splitlines() if 'upload:' in line]

        return []


# Generic class for HTTP and HTTPS server
class HTTPserver(FTPserver):

    # Definitions of data centers in control file
    def __init__(self, configuration):
        super().__init__(configuration)
        self.url = '{}://{}'.format(configuration.get('protocol', 'https'), configuration.get('url', ''))
        self.jar = requests.cookies.RequestsCookieJar()
        self.first_page = configuration.get('first_page', '')
        self.verify_ssl = configuration.get('verify_ssl', True)
        # Define html parser for this server
        parser = configuration.get('parser', 'generic_parser')
        self.parser = getattr(self, parser if hasattr(self, parser) else 'generic_parser')

    def try2connect(self):
        try:
            self.session = requests.Session()
            self.session.mount(self.protocol + '://', TLSAdapter(pool_connections=100, pool_maxsize=100))

            # Connect to first page in case a login is required
            rsp = self.session.get(urljoin(self.url, self.first_page))
            for r in rsp.history:
                self.jar.update(r.cookies)
            if rsp.status_code == 200:
                self.connected = True
                return True
            self.add_error('could not connect to {} [{}]'.format(self.url, str(rsp.status_code)))

        except Exception as err:
            self.add_error('could not connect to {} [{}]'.format(self.url, str(err)))
        return False

    # Connect once to server and login. Set required cookies so login is not required for each request.
    def connect(self):
        for iteration in range(3):
            if self.try2connect():
                return
            self.add_error(f'connect to {self.url} iter {iteration}', is_error=False)
            time.sleep(5)

    # Loop all columns to find datetime compatible string
    def decode_web_time(self, row):
        for col in row.find_all('td'):
            try:
                local_time = self.tz.localize(datetime.strptime(col.text.strip(), '%Y-%m-%d %H:%M'))
                time_value = local_time.astimezone(pytz.UTC)
                break
            except ValueError:
                pass
        else:
            time_value = self.T0  # Fake date
        # Return timestamp
        return int(time_value.timestamp())

    # Generic parser for most of http servers
    def generic_parser(self, content):
        groups = {'[   ]': 'file', '[DIR]': 'dir'}
        # Get all folders
        page = BeautifulSoup(content, 'html.parser')
        for row in page.find_all('tr'):
            if (img := row.find('img', alt=True)) and (grp := groups.get(img['alt'], None)):
                name = row.find('a')['href'].strip()
                if grp == 'file':
                    self.last_files.append((name, self.decode_web_time(row)))
                elif grp == 'dir':
                    self.last_folders.append(name)

    # Parser specific to SHAO site
    def shao_parser(self, content):
        # Get all folders
        is_db = re.compile('^.*\d{2}[A-Z]{3}\d{2}[A-Z]{2}.*$').match
        for line in BeautifulSoup(content, 'html.parser').text.splitlines():
            if is_db(line):
                name, dmy, hm, *_ = line.strip().split()
                try:
                    date_value = datetime.strptime(f'{dmy} {hm}', '%d-%b-%Y %H:%M')
                except:
                    date_value = self.T0
                self.last_files.append((name, int(date_value.timestamp())))

    # Parser specific to EathData https site
    def earthdata_parser(self, content):
        # Get all folders
        page = BeautifulSoup(content, 'html.parser')
        for item in page.find_all(attrs={'class': 'archiveDirText'}):
            self.last_folders.append(item.get('href'))

        # Get all files
        for item in page.find_all(attrs={'class': 'archiveItemTextContainer'}):
            name = item.get('href').strip()
            local_time = datetime.strptime(item.find(attrs={'class': 'fileInfo'}).text[0:19], '%Y:%m:%d %H:%M:%S')
            timestamp = int(self.tz.localize(local_time).astimezone(pytz.UTC).timestamp())
            self.last_files.append((name, timestamp))

    # No parser available
    def no_parser(self, content):
        self.add_error('no parser')
        return [], []

    # List directory and decode information using specific parser
    def listdir(self, folder, reset=True):
        # Check if reading the same folder
        if not reset and folder == self.last_dir:
            return self.last_folders, self.last_files

        self.last_dir, self.last_folders, self.last_files = folder, [], []
        try:
            rsp = self.session.get(urljoin(self.url, folder), cookies=self.jar)
            self.parser(rsp.content)
        except:
            pass

        return self.last_folders, self.last_files

    # Download file using request and compute md5 checksum
    def download(self, rpath, lpath):
        if not self.is_connected:
            self.add_error(f'{self.code} not connected')
            return False, self.errors
        try:
            md5 = hashlib.md5()
            with self.session.get(urljoin(self.url, rpath), cookies=self.jar, stream=True) as r:
                r.raise_for_status()
                with open(lpath, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:  # filter out keep-alive new chunks
                            md5.update(chunk)
                            f.write(chunk)
            return True, md5.hexdigest()
        except Exception as err:
            self.add_error('download {} failed: [{}]'.format(rpath, str(err)))
            return False, self.errors

class CURLftp(HTTPserver):

    # Connect to server
    def connect(self):
        print('URL', self.url)
        if self.url:
            self.host = pycurl.Curl()
            self.host.setopt(pycurl.FOLLOWLOCATION, True)
            self.host.setopt(pycurl.USE_SSL, self.protocol == 'sftp')
            self.host.setopt(pycurl.FTP_USE_EPSV, False)
            self.connected = True

    # List files in directory with their timestamp
    def listdir(self, folder, reset=True):

        # Check if reading the same folder
        if not reset and folder == self.last_dir:
            return self.last_folders, self.last_files

        self.last_dir, self.last_folders, self.last_files = folder, [], []
        buffer = BytesIO()
        protocol = 'ftp' if self.protocol == 'sftp' else self.protocol
        base_url = f'{protocol}://{self.url}'
        print(folder)
        print(base_url)
        url = urljoin(base_url, folder)
        print(url)
        self.host.setopt(pycurl.URL, url)
        self.host.setopt(pycurl.WRITEFUNCTION, buffer.write)
        self.host.perform()

        for line in buffer.getvalue().decode('utf-8').splitlines():
            info = line.split()
            if info[0].startswith('d'):
                self.last_folders.append(info[-1])
            else:
                self.last_files.append((info[-1], self.decode_ftptime(' '.join(info[-4:-1]))))
        return self.last_folders, self.last_files

    # Download file and compute MD5 check sum
    def download(self, rpath, lpath):
        if not self.is_connected:
            return False, 'connection closed'
        try:
            base_url = f'{"ftp" if self.protocol == "sftp" else self.protocol}://{self.url}'
            print(rpath)
            print(base_url)
            url = urljoin(base_url, os.path.join(self.root, rpath))
            print(url)
            with open(lpath, 'wb') as f:
                md5 = hashlib.md5()
                def process(chunk):
                    md5.update(chunk)
                    f.write(chunk)
                self.host.setopt(pycurl.URL, url)
                self.host.setopt(pycurl.WRITEFUNCTION, process)
                self.host.perform()

                return True, md5.hexdigest()
        except Exception as err:
            self.add_error('download {} failed : [{}]'.format(rpath, str(err)))
            return False, self.errors


class CURLhttp(HTTPserver):

    # Connect once to server and login. Set required cookies so login is not required for each request.
    def connect(self):
        if self.url:
            self.host = pycurl.Curl()
            netrc_file = os.path.expanduser('~/.netrc')
            cookie_file = os.path.expanduser(f'~/.{self.code}_cookies')
            self.host.setopt(pycurl.URL, self.url)
            self.host.setopt(pycurl.FOLLOWLOCATION, True)
            self.host.setopt(pycurl.NETRC_FILE, netrc_file)
            self.host.setopt(pycurl.NETRC, True)  # needed?
            self.host.setopt(pycurl.COOKIEFILE, cookie_file)
            self.host.setopt(pycurl.COOKIEJAR, cookie_file)
            self.host.setopt(pycurl.SSL_CIPHER_LIST, "DEFAULT:@SECLEVEL=1")
            self.host.setopt(pycurl.WRITEFUNCTION, lambda data: len(data))
            if not self.verify_ssl:
                self.host.setopt(pycurl.SSL_VERIFYPEER, 0)
                self.host.setopt(pycurl.SSL_VERIFYHOST, 0)

            self.host.perform()

    def exists(self, rpath, reset=True):
        url = urljoin(self.url, rpath)
        print('EXISTS', url)
        buffer = BytesIO()
        try:
            self.host.setopt(pycurl.URL, url)
            self.host.setopt(pycurl.NOBODY, True)
            self.host.setopt(pycurl.HEADER, True)
            self.host.setopt(pycurl.HEADERFUNCTION, buffer.write)
            self.host.setopt(pycurl.WRITEFUNCTION, lambda data: len(data))
            # Perform the request
            self.host.perform()
            # HTTP response code, e.g. 200.
            status = self.host.getinfo(self.host.RESPONSE_CODE)
            print('STATUS', status)
            print('HEADER', buffer.getvalue().decode('utf-8'))
        except Exception as err:
            print('ERR', str(err))

    # List directory and decode information using specific parser
    def listdir(self, folder, reset=True):
        # Check if reading the same folder
        if not reset and folder == self.last_dir:
            return self.last_folders, self.last_files

        self.last_dir, self.last_folders, self.last_files = folder, [], []
        try:
            buffer = BytesIO()
            print('LISTDIR', folder)
            url = urljoin(self.url, folder)
            print(url)
            self.host.setopt(pycurl.URL, url)
            self.host.setopt(pycurl.WRITEFUNCTION, buffer.write)
            self.host.perform()

            print('LISTDIR', self.host.getinfo(self.host.RESPONSE_CODE))
            self.parser(buffer.getvalue().decode('utf-8'))
        except Exception as err:
            print(str(err))

        return self.last_folders, self.last_files


# Load configurations for all servers in config file
def load_servers(category=None):
    global configurations
    global categories
    global last_mod_time

    try:
        mod_time, info = app.load_control_file(name=app.ControlFiles.Servers, lastmod=last_mod_time)
        if info:
            configurations = {grp: info[grp] for grp in categories}
            last_mod_time = mod_time
        return list(configurations[category].keys()) if category in categories else []
    except:
        return []


# Get list of centers for a specific category
def get_centers(category):
    global configurations
    global categories

    return list(configurations[category].keys()) if category in categories else []


# Get configuration of a specific server
def get_config_item(category, center, item, default=''):
    global configurations
    try:
        return configurations[category][center][item]
    except:
        return default

# Get ftp or http server
def get_server(category, code):
    global configurations

    try:
        config = configurations[category][code]
        if config.get('method', 'default') == 'curl':
            return CURLhttp(config) if config['protocol'] in ['http', 'https'] else CURLftp(config)
        return HTTPserver(config) if config['protocol'] in ['http', 'https'] else FTPserver(config)
    except Exception as err:
        return FTPserver({})


if __name__ == '__main__':

    import argparse

    def test_server(category, code, folder):
        with get_server(category, code) as server:
            print(code, server.code, server.root, server.connected, server.errors)
            recent = os.path.join(server.root, folder)
            for (name, path, timestamp) in server.walk(recent):
                print(code, server.code, name, path, server.exists(path), server.get_timestamp(path, reset=False))

    def test_download(category, code, path):
        with get_server(category, code) as server:
            print(code, server.code, server.root, server.connected, server.errors)
            lpath = os.path.join('/tmp', '_'+os.path.basename(path))
            ans = server.download(path, lpath)
            print(ans)


    def test_mets(category, code, station, obs_date):
        year, doy = datetime.strptime(obs_date, '%Y-%m-%d').strftime('%Y %j').split()
        with get_server(category, code) as server:
            print(code, server.code, server.root, server.connected, server.errors)
            rpath = os.path.join(server.root, server.file_name.format(year=year, station=station, doy=doy))
            print(rpath)
            lpath = os.path.join('/tmp', os.path.basename(rpath))
            print(rpath, lpath)
            ans = server.download(rpath, lpath)
            print(ans)

    parser = argparse.ArgumentParser( description='Web pages updater.' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    #parser.add_argument('category')
    #parser.add_argument('code')

    args = app.init(parser.parse_args())

    for i in range(5):
        load_servers()

    test_server('DataCenter', 'cddis', 'RECENT/ivsdata/vgosdb/2021')
    test_server('DataCenter', 'earthdata', 'RECENT/ivsdata/vgosdb/2021')
    #test_server('DataCenter', 'bkg', 'RECENT/ivsdata/vgosdb/2021')
    #test_server('DataCenter', 'opar', 'RECENT/ivsdata/aux/2021')
    test_server('Correlator', 'wien', '')
    test_download('Server', 'vlba', '/astro/VOBS/IVS/2021/q21037/q21037.vex')
    test_server('Correlator', 'utas', '')
    #test_download('Server', 'massloading', '/imsl/load_list/atm/merra2/bds_summary.txt')
    #test_server('Server', 'vmf', '2021')
    #test_server('Correlator', 'vien', '')
    #test_mets('Server', 'ibge', 'ceeu', '2021-05-21')

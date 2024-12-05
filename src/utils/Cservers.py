import os
import ssl
import re
import time
from datetime import datetime, timedelta
import pycurl
from io import BytesIO

from ftplib import FTP_TLS, FTP
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from bs4 import BeautifulSoup
import pytz
import hashlib
import requests

from utils import app

# Define globals variables
configurations = {}
DATACENTER = 'DataCenter'
CORRELATOR = 'Correlator'
SERVER = 'Server'

categories = [DATACENTER, CORRELATOR, SERVER]
last_mod_time = None


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
        self.url = f'{"ftp" if self.protocol == "sftp" else self.protocol}://{configuration.get("url", "")}'
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
        print('URL', self.url)

        self.curl = None

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

    # Connect to server
    def connect(self):
        if not self.curl:
            self.curl = pycurl.Curl()
            self.curl.setopt(pycurl.FOLLOWLOCATION, True)
            self.curl.setopt(pycurl.USE_SSL, self.protocol == 'sftp')
            self.connected = True
            print(self.code, self.url, 'connected')

    # Close connection
    def close(self):
        try:
            self.curl.close()
        except:
            pass
        self.connected = False

    # Upload file to ivs center (This is specific to each server)
    def no_upload(self, lst, testing=False):
        self.add_error(f'cannot upload to {self.code}')
        return 0

    # List files in directory with their timestamp
    def listdir(self, folder):

        folder = folder if folder.endswith('/') else folder + '/'
        self.last_dir, self.last_folders, self.last_files = folder, [], []
        buffer = BytesIO()
        self.curl.setopt(pycurl.URL, urljoin(self.url, folder))
        self.curl.setopt(pycurl.NOBODY, False)
        self.curl.setopt(pycurl.HEADER, False)
        self.curl.setopt(pycurl.WRITEFUNCTION, buffer.write)
        self.curl.perform()

        # Decode listing
        for line in buffer.getvalue().decode('utf-8').splitlines():
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
        return int(time_value.timestamp())

    # Try another format for decoding time
    def decode_old_ftptime(self, text):
        try:
            return self.tz.localize(datetime.strptime(text, '%b %d %Y'))
        except:
            return self.T0  # Could not decode time

    # Detect if file exist and provide timestamp
    def get_file_info(self, rpath):
        if not self.is_connected:
            return False, 0

        print('INFO')
        # Request file time
        self.curl.setopt(pycurl.OPT_FILETIME, True)
        # Just get the header info, not the actual file
        self.curl.setopt(pycurl.NOBODY, True)
        self.curl.setopt(pycurl.HEADER, False)
        self.curl.setopt(pycurl.URL, urljoin(self.url, rpath))
        #self.curl.setopt(pycurl.HEADERFUNCTION, lambda data: len(data))
        self.curl.setopt(pycurl.WRITEFUNCTION, lambda data: len(data))
        self.curl.perform()
        # HTTP response code, e.g. 200
        print('INFO end')
        try:
            file_time = self.tz.localize(datetime.fromtimestamp(self.curl.getinfo(pycurl.INFO_FILETIME)))
            return True, int(file_time.timestamp())
        except Exception as err:
            print('INFO', str(err))
            return False, 0

    # Get timestamp for specific file
    def get_timestamp(self, path):
        return self.get_file_info(path)[1]

    # Check if file exists
    def exists(self, path):
        return self.get_file_info(path)[0]

    # Download file and compute MD5 check sum
    def download(self, rpath, lpath):
        if not self.is_connected:
            return False, 'connection closed'
        try:
            with open(lpath, 'wb') as f:
                md5 = hashlib.md5()
                def process(chunk):
                    print('CHUNK', len(chunk))
                    md5.update(chunk)
                    f.write(chunk)
                    return len(chunk)

                self.curl.setopt(pycurl.URL, urljoin(self.url, rpath))
                print(urljoin(self.url, rpath))
                self.curl.setopt(pycurl.NOBODY, False)
                self.curl.setopt(pycurl.HEADER, False)
                self.curl.setopt(pycurl.WRITEFUNCTION, process)
                self.curl.perform()

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
        #uploader.mount('https://', TLSAdapter(pool_connections=100, pool_maxsize=100))

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

    # Connect once to server and login. Set required cookies so login is not required for each request.
    def connect(self):
        if not self.curl:
            self.curl = pycurl.Curl()
            netrc_file = os.path.expanduser('~/.netrc')
            cookie_file = os.path.expanduser(f'~/.{self.code}_cookies')
            self.curl.setopt(pycurl.URL, self.url)
            self.curl.setopt(pycurl.FOLLOWLOCATION, True)
            self.curl.setopt(self.curl.NETRC_FILE, netrc_file)
            self.curl.setopt(self.curl.NETRC, True)  # needed?
            self.curl.setopt(self.curl.COOKIEFILE, cookie_file)
            self.curl.setopt(self.curl.COOKIEJAR, cookie_file)
            self.curl.setopt(self.curl.SSL_CIPHER_LIST, "DEFAULT:@SECLEVEL=1")
            if not self.verify_ssl:
                self.curl.setopt(pycurl.SSL_VERIFYPEER, 0)
                self.curl.setopt(pycurl.SSL_VERIFYHOST, 0)
            self.curl.setopt(pycurl.WRITEFUNCTION, lambda data: len(data))

            self.curl.perform()

            print('Connect status', self.curl.getinfo(self.curl.RESPONSE_CODE))
            self.connected = self.curl.getinfo(self.curl.RESPONSE_CODE) == 200
            print('Connect', self.is_connected)


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
            name = item.find(attrs={'class': 'archiveItemText'}).get('href').strip()
            local_time = datetime.strptime(item.find(attrs={'class': 'fileInfo'}).text[0:19], '%Y:%m:%d %H:%M:%S')
            timestamp = int(self.tz.localize(local_time).timestamp())
            self.last_files.append((name, timestamp))

    # No parser available
    def no_parser(self, content):
        self.add_error('no parser')
        return [], []

    # List directory and decode information using specific parser
    def listdir(self, folder):
        self.last_dir, self.last_folders, self.last_files = folder, [], []
        try:
            buffer = BytesIO()
            base_url = self.url #f'{self.protocol}://{self.url}'
            print('LISTDIR', folder)
            print(base_url)
            url = urljoin(base_url, folder)
            print(url)
            self.curl.setopt(pycurl.URL, url)
            self.curl.setopt(pycurl.WRITEFUNCTION, buffer.write)
            self.curl.perform()

            print('LISTDIR', self.curl.getinfo(self.curl.RESPONSE_CODE))
            html = buffer.getvalue().decode('utf-8')
            #print(html)
            self.parser(buffer.getvalue().decode('utf-8'))
        except Exception as err:
            print(str(err))

        return self.last_folders, self.last_files

    def get_file_info(self, rpath):
        url = urljoin(self.url, rpath)
        print('EXISTS', url)
        buffer = BytesIO()
        try:
            self.curl.setopt(pycurl.URL, url)
            self.curl.setopt(pycurl.NOBODY, True)
            self.curl.setopt(pycurl.HEADER, True)
            self.curl.setopt(pycurl.HEADERFUNCTION, buffer.write)
            self.curl.setopt(pycurl.WRITEFUNCTION, lambda data: len(data))
            # Perform the request
            self.curl.perform()
            # HTTP response code, e.g. 200.
            print('HEADER', buffer.getvalue().decode('utf-8'))
            for line in buffer.getvalue().decode('utf-8').splitlines():
                if line.startswith('Last-Modified:'):
                    file_time = line.split(':', 1)[-1].strip()
                    zone = pytz.timezone(file_time.split()[-1].strip())
                    print(file_time, zone)
                    return True, int(zone.localize(datetime.strptime(file_time, '%a, %d %b %Y %H:%M:%S %Z')).timestamp())
        except Exception as err:
            print('ERR', str(err))
        return False, 0

    # Download file using request and compute md5 checksum
    def download_old(self, rpath, lpath):
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
                print(code, server.code, name, path, server.exists(path), server.get_timestamp(path))

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

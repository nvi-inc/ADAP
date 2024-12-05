import os
import ssl
import re
import time
import traceback
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
DATACENTER, CORRELATOR, SERVER = ('DataCenter', 'Correlator', 'Server')

categories = [DATACENTER, CORRELATOR, SERVER, 'Alias']
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
    TIMEfmt = '%a, %d %b %Y %H:%M:%S %Z'

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
    def listdir(self, folder):
        lines = []
        try:
            self.host.dir(folder, lines.append)
        except Exception as err:
            self.add_error(str(err))

        files, folders = [], []
        for line in lines:
            info = line.split()
            if info[0].startswith('d'):
                folders.append(info[-1])
            else:
                files.append((info[-1], self.decode_ftptime(' '.join(info[-4:-1]))))
        return folders, files

    # Decode time stamp
    def decode_ftptime(self, text):
        try:
            now = datetime.now(self.tz) + timedelta(seconds=120)  # In case servers are not sync
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
    def get_file_info(self, path, nbr_tries=0):
        if not self.is_connected or nbr_tries > 3:
            return False, 0
        _, files = self.listdir(os.path.dirname(path))
        timestamp = dict(files).get(os.path.basename(path), 0)
        if timestamp > 0:
            return True, timestamp
        time.sleep(1)
        return self.get_file_info(path, nbr_tries+1)

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
                    md5.update(chunk)
                    f.write(chunk)

                self.host.retrbinary(f'RETR {rpath}', process)
                return True, md5.hexdigest()
        except Exception as err:
            self.add_error('download {} failed : [{}]'.format(rpath, str(err)))
            return False, self.errors

    # Walk function using lisdir
    def _walk(self, directory):
        sub_dirs, files = self.listdir(directory)

        yield(directory, files)

        for subdir in sub_dirs:
            for x in self._walk(os.path.join(directory, subdir)):
                yield x

    # Walk through all directories under top and extract all files with their timestamp
    def walk(self, top, reject=[]):
        if self.is_connected:
            for root, files in self._walk(top):
                for filename, timestamp in files:
                    if filename not in reject:
                        yield filename, os.path.join(root, filename), timestamp

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
            self.session.headers = {'UserAgent': 'Mozilla/5.0 (X11; Linux; rv:74.0) Gecko/20100101 Firefox/74.0'}

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

    # Loop all columns to find datetime compatible string
    def decode_web_time(self, row):
        for col in row.find_all('td'):
            try:
                time_value = self.tz.localize(datetime.strptime(col.text.strip(), '%Y-%m-%d %H:%M'))
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
        folders, files = [], []
        for row in page.find_all('tr'):
            if (img := row.find('img', alt=True)) and (grp := groups.get(img['alt'], None)):
                name = row.find('a')['href'].strip()
                if grp == 'file':
                    files.append((name, self.decode_web_time(row)))
                elif grp == 'dir':
                    folders.append(name)
        return folders, files

    # Parser specific to SHAO site
    def shao_parser(self, content):
        files = []
        # Get all files
        is_db = re.compile('^.*\d{2}[A-Z]{3}\d{2}[A-Z]{2}.*$').match
        for line in BeautifulSoup(content, 'html.parser').text.splitlines():
            if is_db(line):
                name, dmy, hm, *_ = line.strip().split()
                try:
                    date_value = datetime.strptime(f'{dmy} {hm}', '%d-%b-%Y %H:%M')
                except:
                    date_value = self.T0
                files.append((name, int(date_value.timestamp())))
        return [], files

    # Parser specific to EathData https site
    def earthdata_parser(self, content):
        folders, files = [], []
        # Get all folders
        page = BeautifulSoup(content, 'html.parser')
        for item in page.find_all(attrs={'class': 'archiveDirText'}):
            folders.append(item.get('href'))

        # Get all files
        for item in page.find_all(attrs={'class': 'archiveItemTextContainer'}):
            name = item.find(attrs={'class': 'archiveItemText'}).get('href').strip()
            local_time = datetime.strptime(item.find(attrs={'class': 'fileInfo'}).text[0:19], '%Y:%m:%d %H:%M:%S')
            files.append((name, int(self.tz.localize(local_time).timestamp())))
        return folders, files

    # No parser available
    def no_parser(self, content):
        self.add_error('no parser')
        return [], []

    # List directory and decode information using specific parser
    def listdir(self, folder):
        try:
            rsp = self.session.get(urljoin(self.url, folder), cookies=self.jar)
            return self.parser(rsp.content)
        except:
            pass

        return [], []

    # Download file using request and compute md5 checksum
    def get_file_info(self, rpath, nbr_tries=0):
        if not self.is_connected or nbr_tries > 3:
            self.add_error(f'{self.code} not connected {nbr_tries}')
            return False, 0
        try:
            with self.session.head(urljoin(self.url, rpath), cookies=self.jar, stream=True) as r:
                if r.status_code != 200:
                    return False, 0
                file_time = r.headers['Last-Modified'].strip()
                zone = pytz.timezone(file_time.split()[-1].strip())
                return True, int(zone.localize(datetime.strptime(file_time, self.TIMEfmt)).timestamp())
        except Exception as err:
            return super().get_file_info(rpath, nbr_tries)

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

    def __init__(self, configuration):
        super().__init__(configuration)
        self.url = f'ftp://{configuration.get("url", "")}'

    # Connect to server
    def try2connect(self):
        self.host = pycurl.Curl()
        self.host.setopt(pycurl.FOLLOWLOCATION, True)
        self.host.setopt(pycurl.USE_SSL, self.protocol == 'sftp')
        self.host.setopt(pycurl.FTP_USE_EPSV, False)
        self.connected = True
        return True

    # Detect if file exist and provide timestamp
    def get_file_info(self, rpath):
        # Request file time
        self.host.setopt(pycurl.OPT_FILETIME, True)
        # Just get the header info, not the actual file
        self.host.setopt(pycurl.NOBODY, True)
        self.host.setopt(pycurl.HEADER, False)
        self.host.setopt(pycurl.URL, urljoin(self.url, rpath))
        self.host.setopt(pycurl.WRITEFUNCTION, lambda data: len(data))
        self.host.perform()
        # Decode time
        try:
            file_time = self.tz.localize(datetime.fromtimestamp(self.host.getinfo(pycurl.INFO_FILETIME)))
            return True, int(file_time.timestamp())
        except Exception as err:
            return False, 0

    # List files in directory with their timestamp
    def listdir(self, folder):

        folder = folder if folder.endswith('/') else folder + '/'

        buffer = BytesIO()
        self.host.setopt(pycurl.URL, urljoin(f'ftp:://{self.url}', folder))
        self.host.setopt(pycurl.NOBODY, False)
        self.host.setopt(pycurl.HEADER, False)
        self.host.setopt(pycurl.WRITEFUNCTION, buffer.write)
        self.host.perform()

        # Decode listing
        folders, files = [], []
        for line in buffer.getvalue().decode('utf-8').splitlines():
            info = line.split()
            if info[0].startswith('d'):
                folders.append(info[-1])
            else:
                files.append((info[-1], self.decode_ftptime(' '.join(info[-4:-1]))))

        return folders, files

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
                    return len(chunk)

                self.host.setopt(pycurl.URL, urljoin(self.url, rpath))
                self.host.setopt(pycurl.NOBODY, False)
                self.host.setopt(pycurl.HEADER, False)
                self.host.setopt(pycurl.WRITEFUNCTION, process)
                self.host.perform()

                return True, md5.hexdigest()
        except Exception as err:
            self.add_error('download {} failed : [{}]'.format(rpath, str(err)))
            return False, self.errors


class CURLhttp(CURLftp):

    def __init__(self, configuration):
        super(CURLhttp, self).__init__(configuration)

        self.url = f'{configuration.get("protocol", "https")}://{configuration.get("url", "")}'
        self.verify_ssl = configuration.get('verify_ssl', True)


    # Connect once to server and login. Set required cookies so login is not required for each request.
    def try2connect(self):
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
        if not self.verify_ssl:
            self.host.setopt(pycurl.SSL_VERIFYPEER, 0)
            self.host.setopt(pycurl.SSL_VERIFYHOST, 0)
        self.host.setopt(pycurl.WRITEFUNCTION, lambda data: len(data))

        self.host.perform()

        self.connected = self.host.getinfo(pycurl.RESPONSE_CODE) == 200
        return self.connected

    # List directory and decode information using specific parser
    def listdir(self, folder):
        try:
            buffer = BytesIO()
            self.host.setopt(pycurl.URL, urljoin(self.url, folder))
            self.host.setopt(pycurl.WRITEFUNCTION, buffer.write)
            self.host.perform()
            return self.parser(buffer.getvalue().decode('utf-8'))
        except Exception:
            return [], []

    def get_file_info(self, rpath):
        try:
            buffer = BytesIO()
            self.host.setopt(pycurl.URL, urljoin(self.url, rpath))
            self.host.setopt(pycurl.NOBODY, True)
            self.host.setopt(pycurl.HEADER, True)
            self.host.setopt(pycurl.HEADERFUNCTION, buffer.write)
            self.host.setopt(pycurl.WRITEFUNCTION, lambda data: len(data))
            # Perform the request
            self.host.perform()
            # Decode time record
            for line in buffer.getvalue().decode('utf-8').splitlines():
                if line.startswith('Last-Modified:'):
                    file_time = line.split(':', 1)[-1].strip()
                    zone = pytz.timezone(file_time.split()[-1].strip())
                    return True, int(zone.localize(datetime.strptime(file_time, self.TIMEfmt)).timestamp())
        except Exception:
            return False, 0


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
    except Exception as exc:
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
        if config.get('method', '') == 'curl':
            return CURLhttp(config) if config['protocol'] in ['http', 'https'] else CURLftp(config)
        return HTTPserver(config) if config['protocol'] in ['http', 'https'] else FTPserver(config)
    except Exception as err:
        return FTPserver({})

# Get ftp or http server
def get_aliases(category, code):
    global configurations

    codes = [code] if code in configurations[category] else []
    codes.extend(configurations.get('Alias', {}).get(category, {}).get(code, []))
    return codes


if __name__ == '__main__':

    import argparse

    def test_server(category, code, folder):

        with get_server(category, code) as server:
            index = 1
            t1 = datetime.now()
            recent = os.path.join(server.root, folder)
            for (name, path, timestamp) in server.walk(recent):
                print(index, datetime.now() - t1, code, server.code, name, path, timestamp)
                index += 1

    def test_download(category, code, path):
        with get_server(category, code) as server:
            if server.exists(path):
                lpath = os.path.join('/tmp', f'{code}_'+os.path.basename(path))
                ok, info = server.download(path, lpath)
                print(code, path, lpath, ok, info)
            else:
                print(code, path, 'does not exist!')


    def test_mets(category, code, station, obs_date):
        year, doy = datetime.strptime(obs_date, '%Y-%m-%d').strftime('%Y %j').split()
        with get_server(category, code) as server:
            rpath = os.path.join(server.root, server.file_name.format(year=year, station=station, doy=doy))
            lpath = os.path.join('/tmp', os.path.basename(rpath))
            ok, info = server.download(rpath, lpath)
            print(rpath, lpath, ok, info)

    parser = argparse.ArgumentParser( description='Web pages updater.' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('action')
    #parser.add_argument('code')

    args = app.init(parser.parse_args())

    load_servers()

    if args.action == 'data':
        test_server(DATACENTER, 'earthdata', 'RECENT/ivsdata/aux/2022')
        test_server(DATACENTER, 'cddis', 'RECENT/ivsdata/aux/2022')
        test_server(DATACENTER, 'bkg', 'RECENT/ivsdata/aux/2022')
        test_server(DATACENTER, 'opar', 'RECENT/ivsdata/aux/2022')
    elif args.action == 'corr':
        test_server(CORRELATOR, 'bonn', '')
        test_server(CORRELATOR, 'gsi', '')
        test_server(CORRELATOR, 'hays', '')
        #test_server(CORRELATOR, 'shao', '')
        test_server(CORRELATOR, 'wien', '')
        test_server(CORRELATOR, 'vien', '')
        test_server(CORRELATOR, 'utas', '')
    elif args.action == 'download':
        test_mets(SERVER, 'ibge', 'ceeu', '2021-05-21')
        test_download(SERVER, 'vlba', '/astro/VOBS/IVS/2021/q21037/q21037.vex')
        test_download(SERVER, 'massloading', '/imsl/load_bds/atm/merra2/bds_summary.txt')
        test_download(SERVER, 'vmf', '/trop_products/VLBI/VMF3/VMF3_OP/daily/2022/2022001.vmf3_r')
        test_download(DATACENTER, 'earthdata', '/archive/vlbi/ivsdata/aux/2022/r11035/r11035.skd')
        test_download(DATACENTER, 'cddis', '/pub/vlbi/ivsdata/aux/2022/r11035/r11035.skd')
        test_download(DATACENTER, 'bkg', '/pub/vlbi/ivsdata/aux/2022/r11035/r11035.skd')
        test_download(DATACENTER, 'opar', '/pub/vlbi/ivsdata/aux/2022/r11035/r11035.skd')
        test_download(CORRELATOR, 'bonn', '/geoarchive/database/21OCT04XA.tgz')
        test_download(CORRELATOR, 'gsi', '/vlbi/data/db/22JAN02XK.tgz')
        test_download(CORRELATOR, 'hays', '/pub/bband_data/v15342_K.tar.gz')
        test_download(CORRELATOR, 'wien', '/data/correlation/public/vgosdb/2021/21JUN24VG.tgz')
        test_download(CORRELATOR, 'vien', '/data/correlation/public/vgosdb/2021/21AUG19VG.tgz')
        test_download(CORRELATOR, 'utas', '/jmccallu/vgosDB/21MAR17XA.tgz')
        test_download(CORRELATOR, 'cddis', '/pub/vlbi/ivsdata/vgosdb/2022/22JAN27XU.tgz')
        test_download(CORRELATOR, 'bkg', '/pub/vlbi/ivsdata/vgosdb/2022/22JAN27XU.tgz')
        test_download(CORRELATOR, 'opar', '/pub/vlbi/ivsdata/vgosdb/2022/22JAN27XU.tgz')
        #test_download(CORRELATOR, 'shao', '/vgosDB/20SEP23XA.tgz')
    elif args.action == 'test':
        test_download(DATACENTER, 'earthdata', '/archive/vlbi/ivsdata/aux/2022/r11035/r11035.skd')
        test_download(DATACENTER, 'earthdata', '/archive/vlb/ivsdata/aux/2022/r11035/r11035.skd')

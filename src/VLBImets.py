import os
import traceback
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
from zipfile import ZipFile

from utils import app
from utils.files import remove, chmod
from utils.servers import load_servers, get_server, SERVER
from rmq import Worker
from ivsdb import IVSdata

class MetScanner(Worker):

    def __init__(self):
        super().__init__()

        self.accepted, self.stations = ['igs'], {}
        # Load Server control file to initialize met data
        self.lastmod = self.check_control_file(None)
        # Get number of days to go back and maximum timeout
        self.days = app.args.days
        self.exclusive_queue = True # Create an exclusive queue that will delete when finished.
        self.set_start_time(app.args.start, app.args.period, reset_timeout=False)

    # Check if control file has been updated and reset information
    def check_control_file(self, lastmod):
        servers = load_servers(SERVER)
        lastmod, config = app.load_control_file(name=app.ControlFiles.Mets, lastmod=lastmod)
        if config:
            self.stations = {}
            for sta, info in config['Station'].items():
                if (data_type := info.get('type', 'igs').lower()) not in self.accepted:
                    self.notify(f'{sta} does not have a valid data type {data_type}')
                elif (center := info.get('center', '')) not in servers:
                    self.notify(f'{sta} does not have a valid data center {center}')
                else:
                    self.stations[sta] = info

        return lastmod

    # Download zip file and extract the met file
    @staticmethod
    def get_met_igs(server, station, folder, year, doy):
        name = f'{station}{doy}1.{int(year)%100:02d}m'
        lpath = os.path.join(folder, name)
        if os.path.exists(lpath):
            return None  # File already exists

        # Get path of remote zip file, download in temporary folder and extract met file
        path = server.file_name.format(year=year, station=station, doy=doy)
        rpath = os.path.join(server.root, path)
        with NamedTemporaryFile(delete=False) as tmp:
            zip = tmp.name
        try:
            if ok := server.download(rpath, zip)[0]:
                with ZipFile(zip) as z:
                    with open(lpath, 'wb') as f:
                        f.write(z.read(name))
                    chmod(lpath)
        except:
            ok = False
        # Remove zip file
        remove(zip)
        return lpath if ok else None

    # Define generic get_met routine for not defined data_types
    @staticmethod
    def get_met_dummy(server, station, folder, year, doy):
        return None

    # Get list of stations that need met data
    def get_stations(self, stations, days=7):
        lst = {sta_id: [] for sta_id in stations}
        t2s = lambda t: t.strftime('%Y-%m-%d 00:00:00')  # function to format time
        year_doy = lambda s, n: (s.start + timedelta(days=n)).strftime('%Y %j').split()
        now = datetime.utcnow()
        start, end = t2s(now - timedelta(days=days)), t2s(now)
        url, tunnel = app.get_dbase_info()
        with IVSdata(url, tunnel) as dbase:
            for ses_id in dbase.get_sessions(start, end, ['standard', 'intensive', 'vgos']):
                session = dbase.get_session(ses_id)
                for sta_id in stations:
                    if sta_id in session.stations:
                        for nbr in range(2):
                            year, doy = year_doy(session, nbr)
                            lst[sta_id].append((session.folder, year, doy))
        return lst

    # Find the met data for station list
    def find_mets(self, stations):
        # Do not execute during quiet time
        if self.is_quiet_time():
            return
        # Open data base
        for sta_id, record in self.get_stations(stations, self.days).items():
            info = self.stations[sta_id]
            station, center = info['name'], info['center']
            get_met = getattr(self, f"get_met_{info.get('type', 'dummy').lower()}")
            # Check if met file ready for this station
            with get_server(SERVER, center) as server:
                for folder, year, doy in record:
                    if path := get_met(server, station, folder, year, doy):
                        self.info(f'download {path}')

    # Process message manually input
    def process_msg(self, ch, method, properties, body):
        station = body.decode("utf-8").strip().lower()
        try:
            self.lastmod = self.check_control_file(self.lastmod)
            self.find_mets([station])
        except Exception as err:
            self.notify(f'Problems {str(err)}')

    # Process timeout.
    def process_timeout(self):
        try:
            # Check if control file has changed
            self.lastmod = self.check_control_file(self.lastmod)
            self.find_mets(list(self.stations.keys()))
        except Exception as err:
            self.notify(f'Problems {str(err)}\n{traceback.format_exc()}')


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Check for Met files.')

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-q', '--queue', help='queue name', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-s', '--start', help='start time', default='now', required=False)
    parser.add_argument('-p', '--period', help='repeat period', type=int, default=7200, required=False)
    parser.add_argument('-D', '--days', help='number of days to go back', type=int, default=7, required=False)

    app.init(parser.parse_args())

    worker = MetScanner()
    worker.monit()

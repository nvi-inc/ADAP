import os
import re
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

import requests

from utils import app
from utils.servers import get_server, load_servers, DATACENTER
from utils.files import MasterTypes, remove, is_master
from ivsdb import IVSdata
from rmq import Worker
from web.webdb import WEBdb
from web import ESDWebDev

has_station = re.compile(r'(?P<expid>[a-zA-Z0-9]*)(?P<stnid>[a-zA-Z0-9]{2})(?:_full\.log\.|\.)'
                         r'(?:log|prc|sum|bz2|gz)$').match
has_session = re.compile(r'(?P<expid>[a-zA-Z0-9]*)[-\.]').match
is_vgosdb = re.compile(r'^(?P<year>\d{2})(?P<month>\w{3})(?P<day>\d{2})(?P<db_code>\w{1,2}).tgz$').match
is_vgosdb_new = re.compile(r'(?P<date>\d{8})-(?P<ses_id>\w{4,12})').match
is_ngsdb = re.compile(r'^(?P<year>\d{2})(?P<month>\w{3})(?P<day>\d{2})(?P<db_code>..)_(?:V|N)'
                      r'(?P<version>\d+).gz$').match


class WebUpdater(Worker):

    MAXDELAY = timedelta(minutes=15)  # Maximum delay to refresh
    MAXFILES = 500  # Maximum files needed to generate an update

    def __init__(self):
        super().__init__()

        self.dbase = None

        # Init some variables
        self.stations, self.sessions, self.masters, self.nbr_files = [], [], [], 0

        self.esdweb, self.ws_url, self.ws_db = None, None, None
        self.lastmod = self.check_control_files()

        self.last_update = datetime.now() - timedelta(hours=2)
        self.set_start_time('now', timeout=app.args.period, reset_timeout=True)

    # Check control files to see if they have been updated
    def check_control_files(self, lastmod=None):
        lastmod, info = app.load_control_file(name=app.ControlFiles.IVSweb, lastmod=lastmod)
        if info:
            ws = info['WebService']
            self.ws_url, self.ws_db = ws['url'], ws['database']
            self.esdweb = info.get('ESDWebDev', {})
        return lastmod

    # Check if ivsdb is open and open if not
    def open_ivsdb(self):
        if not self.dbase:
            url, tunnel = app.get_dbase_info()
            self.dbase = IVSdata(url, tunnel)
            self.dbase.open()

    # Check if ivsdb is open and close it
    def close_ivsdb(self):
        if self.dbase:
            self.dbase.close()
            self.dbase = None

    # Reset information for esdweb pages to update
    def reset(self):
        self.stations, self.sessions, self.masters = [], [], []
        self.nbr_files = 0

    def add_recent(self):
        # Add sessions within +/- a week
        first_day = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d 00:00:00')
        last_day = (datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d 00:00:00')
        for ses_id in self.dbase.get_sessions(first_day, last_day, list(MasterTypes.values())):
            self.add_session(ses_id)

    # Add session code in list and update station list
    def add_station(self, sta):
        if sta not in self.stations:
            self.stations.append(sta)

    # Add session code in list and update station list
    def add_session(self, ses_id, update_master=True):
        if ses_id not in self.sessions:
            self.sessions.append(ses_id)
            session = self.dbase.get_session(ses_id)
            for sta in session.stations:
                self.add_station(sta)
            if update_master:
                master = is_master(session.master())
                if master not in self.masters:
                    self.masters.append(master)

    # Check if master file and update session list for that year
    def is_master_file(self, name):
        if not (master := is_master(name)):
            return False
        if master not in self.masters:
            self.masters.append(master)
        for code, _ in self.dbase.get_sessions_from_year(master['year'], masters=[master['type']]):
            self.add_session(code, update_master=False)
        return True

    # Update esdweb data base with ns-codes or master-format files
    def update_web_db(self, center, name, rpath):
        lpath = NamedTemporaryFile(suffix='_'+name,delete=False).name
        # Retrieve file from center since not sure if latest is on server
        load_servers(DATACENTER)
        with get_server(DATACENTER, center) as server:
            rpath = os.path.join(server.root, rpath)
            ok, err = server.download(rpath, lpath)
        if ok:
            db = WEBdb(self.ws_db)
            if name == 'ns-codes.txt':
                ok, err = db.ns_codes(lpath)
            elif name == 'master-format.txt':
                ok, err = db.master_format(lpath)
            remove(lpath)
        if ok:
            self.notify(f'WEB DB updated with {name}')
        else:
            self.add_error(f'WEB DB update with {name} failed!\n{err}')

    # Check of type of file and update station and session lists
    def is_accepted_file(self, name, timestamp):
        def is_dbase():
            return is_vgosdb(name) or is_vgosdb_new(name) or is_ngsdb(name)
        # Check if file has valid session and stations code
        match = has_station(name)
        if match and self.dbase.get_session(match['expid']) and self.dbase.get_station(match['stnid']):
            self.add_session(match['expid'])
            return True
        # Check if file has valid sessions code
        if (match := has_session(name)) and self.dbase.get_session(match['expid']):
            self.add_session(match['expid'])
            return True
        # Check if vgosdb_old or ivsdb file has valid session code
        if is_dbase() and (code := self.dbase.get_db_session_code(name)):
            self.add_session(code)
            self.dbase.update_recent_file(name, timestamp)
            return True

        return False

    # Request html page from esdweb service
    def make_html_page(self, page):
        try:
            if rsp := requests.get(os.path.join(self.ws_url, page)):
                return rsp.text
        except Exception as err:
            self.add_error(f'request {page} failed! {str(err)}')
        self.add_error(f'Could not make {page}')
        return ''

    # Control the update of esdweb pages
    def update_web_pages(self):
        if not self.esdweb.get('update', True):
            return
        # Check if need to update sessions near this date
        if datetime.now() - self.last_update > self.MAXDELAY:
            self.add_recent()
            self.last_update = datetime.now()
        elif not self.nbr_files:
            return

        # Connect to esdwebdev
        with ESDWebDev(self.esdweb) as esd:
            if esd.has_errors:
                self.add_error(f'ESDWebDev errors\n{esd.errors}')
                return

            # Update pages for all stations
            base_page = 'sessions/stations'
            for sta in self.stations:
                page = os.path.join(base_page, sta)
                esd.save(page, self.make_html_page(page))
            esd.save(base_page, self.make_html_page(base_page))

            # Update pages for each affected sessions
            base_page = 'sessions/'
            for ses_id in self.sessions:
                session = self.dbase.get_session(ses_id)
                page = os.path.join(base_page, session.year, ses_id)
                esd.save(page, self.make_html_page(page))

            # Update affected years
            base_page = 'sessions'
            for master in self.masters:
                stype = '' if master['type'] == 'standard' else master['type']
                page = os.path.join(base_page, stype, master['year'])
                esd.save(page, self.make_html_page(page))

            # Update top page
            page = 'sessions'
            esd.save(page, self.make_html_page(page))

            # Add possible errors and log
            if esd.has_errors:
                self.add_error(f'ESDWebDev errors\n{esd.errors}')

            self.info(f'Found {self.nbr_files} files. Updated {len(esd.updated):d} esdweb pages)')
            self.reset()

    # Submit file to local esdweb service that will generate appropriate pages
    def submit_file(self, rpath, timestamp):
        try:
            url = self.ws_url + '/sessions/submit/'
            resp = requests.post(url, files={'index.txt': ('index', f'{rpath} {timestamp}')})
            if resp.status_code == requests.codes.ok and resp.json().get('imported', []):
                self.nbr_files += 1
                return
            self.notify(f'Could not upload {rpath}\n{resp.text}')
        except Exception as err:
            self.notify(f'Could not upload {rpath}\n{str(err)}')

    # Submit master file to local esdweb service to update sqlite3 and generate appropriate esdweb pages
    def submit_master(self, center, name, rpath):
        lpath = NamedTemporaryFile(suffix='_'+name, delete=False).name
        # Retrieve file from center since not sure if latest is on local server
        load_servers(DATACENTER)
        with get_server(DATACENTER, center) as server:
            rpath = os.path.join(server.root, rpath)
            ok, err = server.download(rpath, lpath)
            if not ok:  # Could not download file
                self.notify(f'Could not update {name} {center} - Not downloaded\n{ok} {rpath} {lpath}\n{err}')
            else:
                url = self.ws_url + '/sessions/submit/'
                resp = requests.post(url, files={name: open(lpath)})
                remove(lpath)
                if resp.status_code == requests.codes.no_content:
                    self.nbr_files += 1
                else:
                    self.notify(f'Could not update {name} - Not uploaded\n{resp.statu_code} {resp.text}')

    # Process message received from scanners
    def process_msg(self, ch, method, properties, body):

        try:
            if (record := body.decode('utf-8').strip()) == 'EOT':
                return
            center, name, rpath, timestamp = record.split(',')
            center = 'cddis' if center == 'sftp2cddis' else center

            self.open_ivsdb()

            if name in ['ns-codes.txt', 'master-format.txt']:
                self.update_web_db(center, name, rpath)
            elif self.is_master_file(name):
                self.submit_master(center, name, rpath)
            elif self.is_accepted_file(name, timestamp):
                self.submit_file(rpath, timestamp)
            # Update esdweb if too many files are in memory
            if self.nbr_files > self.MAXFILES:
                self.update_web_pages()
        except (ValueError, Exception) as err:
            self.add_error(str(err))
        self.send_errors()

    # Process timeout.
    def process_timeout(self):
        try:
            self.lastmod = self.check_control_files(self.lastmod)
            # No information received for the last 60 seconds.
            self.open_ivsdb()
            self.update_web_pages()
            self.close_ivsdb()
        except Exception as err:
            self.add_error(str(err))
        self.send_errors()


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser( description='Web pages updater.' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-q', '--queue', help='queue name', required=True)
    parser.add_argument('-p', '--period', help='repeat period', type=int, default=60, required=False)

    app.init(parser.parse_args())
    web = WebUpdater()
    web.monit()

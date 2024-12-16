import os
import requests

from utils import app
from web import ESDWebDev


class WebReset:

    def __init__(self):
        _, info = app.load_control_file(name=app.ControlFiles.IVSweb)
        self.ws_url = info['WebService']['url']
        self.esdweb = info.get('ESDWebDev', {})

    # Request html page from esdweb service
    def make_html_page(self, page):
        try:
            if rsp := requests.get(os.path.join(self.ws_url, page)):
                return rsp.text
        except Exception:
            return None

    def reset(self, first, last):
        with ESDWebDev(self.esdweb) as esd:
            dbase = app.get_dbase()
            if first == 'stations':
                base_page = 'sessions/stations'
                for sta_id in dbase.get_stations():
                    page = os.path.join(base_page, sta_id)
                    esd.save(page, self.make_html_page(page))
                esd.save(base_page, self.make_html_page(base_page))
            else:
                first = int(first)
                last = int(last) if last else first
                base_page = 'sessions/'
                for year in range(first, last + 1):
                    for (ses_id, start) in dbase.get_sessions_from_year(str(year)):
                        page = os.path.join(base_page, str(year), ses_id)
                        esd.save(page, self.make_html_page(page))
                    for stype in ['', 'intensive', 'vgos']:
                        page = os.path.join(base_page, stype, str(year))
                        esd.save(page, self.make_html_page(page))
                # Update top page
                esd.save(base_page, self.make_html_page(base_page))


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser( description='Web pages updater.' )

    parser.add_argument('-c', '--config', help='configuration file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('first')
    parser.add_argument('last', default=None, nargs='?')

    args = app.init(parser.parse_args())
    web = WebReset()
    web.reset(args.first, args.last)

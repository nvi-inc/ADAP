import os
import re
import traceback
from datetime import datetime, timedelta, time

from utils import app
from utils.servers import get_server, load_servers, get_centers, DATACENTER
from utils.mail import build_message, send_message
from rmq import Worker
from ivsdb import IVSdata


# Class used to check missing schedules and send emails to appropriate operations_centers
class CheckSkd(Worker):
    file_codes = ['skd', 'vex']  # Schedule files extension
    schedule_types = ['standard', 'intensive']  # All possible type (vgos) not use in future

    def __init__(self):
        super().__init__()

        # Init missing sessions group by operations centers
        self.missing, self.servers = {}, []
        self.aux_folder = app.Applications.DataCenters['Folders']['aux']
        self.ivscc = self.ops_centers = self.mail_server = self.contact = None
        self.exclusive_queue = True  # Create an exclusive queue that will delete when finished.
        # Maximum timeout
        self.set_start_time(app.args.start, app.args.period, reset_timeout=False)

    # Read control files use in this app.
    def read_control_files(self):
        # Information for emails
        info = app.load_control_file(name=app.ControlFiles.Schedules)[-1]
        self.ivscc, self.ops_centers, self.mail_server = info['IVSCC'], info['OpsCenters'], info['Mail']['server']
        self.contact = re.match('(?P<name>.*)<(?P<address>.*)>$', self.ivscc['contact'])
        # Information on data centers
        load_servers(DATACENTER)

    # Connect to data centers
    def connect_servers(self):
        self.servers = []
        for center in get_centers(DATACENTER):
            server = get_server(DATACENTER, center)
            server.connect()
            self.servers.append(server)

    # Close connections to data centers
    def disconnect_servers(self):
        for server in self.servers:
            server.close()
        self.servers = []

    # Add missing session to missing
    def add_missing(self, ses):
        if ses.operations_center not in self.missing:
            self.missing[ses.operations_center] = []
        self.missing[ses.operations_center].append(ses)

    # Make a private message to the operations center
    def make_private_message(self, today, sessions):
        urgent_message, nbr = False, len(sessions)
        # Create header
        header = f'The following schedule{"s have" if nbr > 1 else " has"} not been submitted to the IVS Data Center'
        html = ['<h3>', header, '</h3>', "<table style = 'font-family: arial, sans-serif; border-collapse: collapse;'>",
                "<tr><th style='align: left'>Status</th><th>Session</th><th>Start time</th></tr>"]
        text, url = ['', header, ''], self.ivscc['url']

        for ses in sessions:
            t = ses.start
            days, weekday = (t - today).days, t.weekday()
            weekend = 2 - weekday if weekday < 2 else 0
            urgent = days - weekend < 4
            color, status = ('#FF0000', 'URGENT') if urgent else ('#000000', 'LATE')
            start = f'{t.strftime("%A,"):10s} {t.strftime("%B %d,"):13s} {t.strftime("%Y at %H:%M UTC")}'
            # html message
            ref = os.path.join(url, ses.year, ses.code.lower())
            html.append('<tr>')
            html.append(f"<td style='color: {color}; padding: 0 10px 0 0;'>{status}</td>")
            html.append(f"<td style='color: {color}; padding: 0 20px 0 0;'>")
            html.append(f"<a href=\"{ref}\">{ses.code.upper()}</a> starts in {days:d} days</td>")
            html.append(f"<td style='color: {color};'>{start}</td>")
            html.append("</tr>")
            # Text message
            text.append(f'{ses.code.upper():6s} starts in {days:d} days : {start} {"*** URGENT ***" if urgent else ""}')
            if urgent:
                urgent_message = True

        # End of html table
        html.append("</table>")
        text.append('')

        # Add footer
        html.append(f"<p>For information please contact <a href='mailto:"
                    f"{self.contact[0]}'>{self.contact['name']}</a></p>")
        html.append(f'<p>Visit <a href=\"{url}\">IVS Sessions page</a></p>')
        text.append(f'For information please contact {self.contact["name"]} at {self.contact["address"]}')
        text.append(f'Visit {url} to see IVS Sessions page')

        # Make subject line
        subject = f'{"URGENT" if urgent_message else "REMINDER"} missing schedule{"s" if nbr > 1 else ""}'

        return nbr, urgent_message, subject, '\n'.join(text), ''.join(html)

    # Send messages for the day
    def send_daily_messages(self):
        # Check number of days before start of sessions
        today = datetime.combine(datetime.utcnow().date(), time.min)
        nbr_missing = 0

        for operations_center, sessions in self.missing.items():
            oc = 'TEST' if app.args.test else operations_center.upper()
            nbr, urgent, subject, text, html = self.make_private_message(today, sessions)
            nbr_missing += nbr

            msg = build_message(self.ivscc['sender'], self.ops_centers[oc], subject, reply=self.ivscc['reply-to'],
                                text=text, html=html, urgent=urgent)
            if err := send_message(self.mail_server, msg):
                self.notify(f'Failed sending message to {oc}.', f'[{err}]\nOperations Center {oc}\n{subject}\n\n{text}',
                            wait=False)
            else:
                self.info(f'{"urgent " if urgent else ""}email sent to {oc}')

        return nbr_missing

    # Check on IVS Data Centers if schedule is available
    def check_session(self, ses):
        # Check if schedule in our session folder
        for code in self.file_codes:
            if ses.file_path(code).exists():
                return
        # Check if skd or vex on any IVS data center
        for server in self.servers:
            if server.connected:
                files = server.listdir(os.path.join(server.root, self.aux_folder, ses.year, ses.code.lower()))[-1]
                for code in self.file_codes:
                    if ses.file_path(code).name in files:
                        return
        self.add_missing(ses)

    # Check for missing schedules
    def check_schedules(self, max_days):
        self.start('checking missing schedules')
        # Read control files in case anything has changed
        self.read_control_files()
        # Reset some variables
        self.missing, now = {}, datetime.now()

        def t2s(t):
            return t.strftime('%Y-%m-%d 00:00:00')  # function to format time

        self.connect_servers()  # Connect to all data centers
        today, later = t2s(now), t2s(now + timedelta(days=max_days+1))
        url, tunnel = app.get_dbase_info()
        with IVSdata(url, tunnel) as dbase:
            for nbr, ses_id in enumerate(dbase.get_sessions(today, later, self.schedule_types), 1):
                self.check_session(dbase.get_session(ses_id))
        self.disconnect_servers()  # Close connection to data centers

        nbr_missing = self.send_daily_messages()
        self.stop(f'checked {nbr:d} schedules and {nbr_missing:d} are missing')

    # Process message from queue
    def process_msg(self, ch, method, properties, body):
        try:
            txt = body.decode('utf-8').strip()
            days = int(txt) if txt.isnumeric() else app.args.days
            self.check_schedules(days)
        except Exception as err:
            self.notify(f'Problem {str(err)}\n{str(traceback.format_exc())}')

    # Process timeout.
    def process_timeout(self):
        try:
            self.check_schedules(app.args.days)
        except Exception as err:
            self.notify(f'Problem {str(err)}\n{str(traceback.format_exc())}')

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Check for missing schedules' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-q', '--queue', help='queue name', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-s', '--start', help='start time', default='23:00', required=False)
    parser.add_argument('-p', '--period', help='repeat period', type=int, default=86400, required=False)
    parser.add_argument('-t', '--test', help='test mode', action='store_true')
    parser.add_argument('-D', '--days', help='days for warning', type=int, default=7, required=False)

    app.init(parser.parse_args())

    worker = CheckSkd()
    worker.monit()

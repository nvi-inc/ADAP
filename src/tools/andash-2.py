import sys
import os
from threading import Event

from datetime import datetime, timedelta, date
from tzlocal import get_localzone
from pytz import utc as UTC

from PyQt5.QtCore import QThread, Qt, pyqtSignal, QObject, QRunnable, pyqtSlot, QThreadPool, QTimer
from PyQt5.QtWidgets import QMainWindow, QApplication, QWidget, QLayout,QFrame, QSizePolicy, QRadioButton, QSpinBox
from PyQt5.QtWidgets import QLabel, QVBoxLayout, QHBoxLayout, QGroupBox, QGridLayout, QPushButton, QCheckBox
from PyQt5.QtGui import QCursor

from utils import app
from ivsdb import IVSdata, models
from vgosdb import VGOSdb
from utils.servers import get_server, load_servers, DATACENTER
from utils import read_app_info, save_app_info

class Timer(QThread):
    action = pyqtSignal(bool)
    def __init__(self, on_timeout, rate=1, initial=-1):
        super().__init__()
        self.rate = rate
        self.initial = initial
        self.stopped = Event()
        self.action.connect(on_timeout)

        self.new_rate = None

    def run(self):
        waiting_time = self.initial if self.initial > -1 else self.rate - datetime.utcnow().timestamp() % 1
        while not self.stopped.wait(waiting_time):
            utc = datetime.utcnow()
            self.action.emit(True)
            self.rate, self.new_rate = (self.new_rate, None) if self.new_rate else (self.rate, self.new_rate)
            dt = utc.timestamp() % 1
            waiting_time = self.rate if dt < 0.001 else self.rate - dt

    def stop(self):
        self.stopped.set()


class FindDataSignals(QObject):
    result = pyqtSignal(list)


class FindData(QRunnable):

    def __init__(self, function, *args, **kwargs):
        super().__init__()

        # Store constructor arguments (re-used for processing)
        self.processing_function = function
        self.args, self.kwargs = args, kwargs
        self.signals, self.stopped = FindDataSignals(), Event()

    @pyqtSlot()
    def run(self):
        # Retrieve args/kwargs here; and fire processing function
        try:
            results = self.processing_function(*self.args, **self.kwargs)
        except Exception as err:
            print('FindData error', str(err))
            results = []
        if not self.stopped.is_set():
            self.signals.result.emit(results)

    def stop(self):
        self.stopped.set()


class HSeparator(QFrame):
    def __init__(self, height=5):
        super().__init__()
        self.setMinimumWidth(1)
        self.setFixedHeight(height)
        self.setFrameShape(QFrame.HLine)
        self.setFrameShadow(QFrame.Sunken)
        self.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Minimum)


# Class for showing all sessions
class ANdash(QMainWindow):

    centered = [False, False, True, True, True, True, True]

    def __init__(self):

        self.appInfo = read_app_info(ANdash)

        load_servers(DATACENTER)

        folders = app.Applications.DataCenters['Folders']
        self.aux_folder, self.vgos_folder = folders['aux'], folders['vgosdb']

        self.show_rapid = self.appInfo.get('ShowRapid', False)
        self.session_type = self.appInfo.get('SessionType', 'all')
        self.manual_mode = self.appInfo.get('ManualRefresh', False)
        self.refresh_rate = self.appInfo.get('RefreshRate', 5)
        self._width, self._height = self.appInfo.get('Size', [900, 100])

        self.processing = Event()
        self.worker = None

        self.full_name = 'Analyst Dashboard'

        self.app = QApplication(sys.argv)

        super().__init__()

        self.setWindowFlags(Qt.WindowCloseButtonHint | Qt.WindowMinimizeButtonHint)
        self.setWindowTitle(self.full_name)
        #self.get_sessions(datetime.now() - timedelta(days=30))
        #self.request_information(False, True)
        # Make application layout
        self.Vlayout = QVBoxLayout()
        self.Vlayout.addLayout(self.make_session_list(self.get_sessions(datetime.now() - timedelta(days=30))))
        self.Vlayout.addLayout(self.make_option_box())
        self.Vlayout.addLayout(self.make_footer_box())
        widget = QWidget()
        widget.setLayout(self.Vlayout)

        print('Call it', datetime.now())

        # Start timer to update information
        self.timer = Timer(self.request_information, self.refresh_rate * 60)
        self.timer.start()

        self.setCentralWidget(widget)

        self.init_pos()

        #QThread.sleep(1)  # Delay to make sure the information has been retrieved
        self.show()
        print('Show', datetime.now())


    # Function to help centering
    def make_label(self, text, centering=False):
        label = QLabel(text)
        if centering:
            label.setAlignment(Qt.AlignCenter)
        return label

    # Make the box containing session list
    def make_session_list(self, sessions):

        labels = [('DB name', False, (0, 0, 2, 1)), ('Session', False, (0, 1, 2, 1)), ('Observed', True, (0, 2, 2, 1)),
                  ('Downloaded', True, (0, 3, 2, 1)), ('Analyzed', True, (0, 4, 2, 1)),
                  ('Submitted', True, (0, 5, 1, 2)), ('Report', True, (1, 5)), ('VGOSdb', True, (1, 6))]
        grid = QGridLayout()
        groupbox = QGroupBox()
        box = self.sessions = QGridLayout()
        for (label, centering, pos) in labels:
            box.addWidget(self.make_label(label, centering), *pos)
        box.addWidget(HSeparator(5), 2, 0, 1, 7)
        for row, ses in enumerate(sessions, 3):
            self.refresh_session_row(row, ses)
        groupbox.setLayout(box)
        grid.addWidget(groupbox)
        return grid

    # Function to help creating many radio buttons
    def make_radiobutton(self, title, action):
        rb = QRadioButton(title)
        rb.setChecked(title.lower() == self.session_type)
        rb.toggled.connect(action)
        return rb

    # Add a row inn the session list box
    def add_session_row(self, row):
        for col in range(self.sessions.columnCount()):
            label = self.make_label('', self.centered[col])
            self.sessions.addWidget(label, row, col)

    # Refresh information in row
    def refresh_session_row(self, row, ses):
        print(ses)
        if not self.sessions.itemAtPosition(row, 0):
            self.add_session_row(row)
        for col, (text, color) in enumerate(ses):
            if item := self.sessions.itemAtPosition(row, col):
                item.widget().setText(text)
                item.widget().setStyleSheet(f"QLabel {{color : {'red' if color else 'black'}; }}")

    # Remove rows in session list
    def remove_session_rows(self, nbr):
        rows = self.sessions.rowCount() - 1
        for row in range(rows, rows - nbr, -1):
            for col in range(self.sessions.columnCount()):
                if layout := self.sessions.itemAtPosition(row,col):
                    layout.widget().setParent(None)
                    #layout.widget().deleteLater()
                    #self.sessions.removeItem(layout)

    # Make box containing various options
    def make_option_box(self):
        hbox = QHBoxLayout()
        hb = QHBoxLayout()
        hb.setSizeConstraint(QLayout.SetFixedSize)
        hb.addWidget(self.make_radiobutton('All', self.change_session_type))
        hb.addWidget(self.make_radiobutton('Standard', self.change_session_type))
        hb.addWidget(self.make_radiobutton('Intensive', self.change_session_type))
        gb = QGroupBox()
        gb.setLayout(hb)
        hbox.addWidget(gb)
        hbox.addSpacing(20)

        widget = QCheckBox('Show rapid sessions')
        widget.setChecked(self.show_rapid)
        widget.toggled.connect(self.show_rapid_changed)
        widget.setToolTip('List latest observed R1/R4 and Intensives')

        hbox.addWidget(widget)

        hbox.addStretch(1)

        gb = QGroupBox()
        hb = QHBoxLayout()
        gb.setLayout(hb)

        cb = QCheckBox('Manual Refresh')
        cb.setChecked(self.manual_mode)
        cb.toggled.connect(self.refresh_manual_mode)
        hb.addWidget(cb)
        hb.addSpacing(10)
        hb.addWidget(QLabel('Refresh Rate(m)'))
        sb = QSpinBox()
        sb.setRange(1, 60)
        sb.setValue(int(self.refresh_rate))
        sb.valueChanged.connect(self.refresh_rate_changed)
        hb.addWidget(sb)
        hbox.addWidget(gb)

        hbox.setContentsMargins(10, 5, 15, 5)
        hbox.setSizeConstraint(QLayout.SetNoConstraint)

        groupbox = QGroupBox()
        groupbox.setLayout(hbox)
        grid = QGridLayout()
        grid.addWidget(groupbox)

        return grid

    # Footer containing button and last refresh
    def make_footer_box(self):
        hbox = QHBoxLayout()
        doneButton = QPushButton("Done")
        doneButton.clicked.connect(self.close)
        hbox.addWidget(doneButton)
        self.manual = QPushButton("Refresh")
        self.manual.clicked.connect(self.request_information)
        self.manual.show() if self.manual_mode else self.manual.hide()
        hbox.addWidget(self.manual)

        hbox.addStretch(1)

        self.last_updated = QLabel(datetime.now().strftime('Last updated %H:%M:%S'))
        self.last_updated.setStyleSheet('color: black')

        hbox.addWidget(self.last_updated)
        hbox.setContentsMargins(10, 5, 15, 5)
        hbox.setSizeConstraint(QLayout.SetNoConstraint)
        return hbox

    # Initial position of application window
    def init_pos(self):
        x, y = self.appInfo.get('Position', [0, 0])
        self.move(x, y)
        self.resize_it()

    # Force window to resize. Height will adjust to data in box
    def resize_it(self):
        self.resize(self._width, self._height)

    # The session type has been changed
    def change_session_type(self):
        self.session_type = self.sender().text().lower()
        self.request_information(False, True)

    # Toggle automatic mode when button is press
    def show_rapid_changed(self):
        self.show_rapid = self.sender().isChecked()
        self.request_information(False)

    # Manual mode check box has changed
    def refresh_manual_mode(self):
        self.manual_mode = self.sender().isChecked()
        self.manual.show() if self.manual_mode else self.manual.hide()

    # Manual mode check box has changed
    def refresh_rate_changed(self, value):
        self.timer.rate = value * 60

    # The window size has changed. Keeping the last width of it
    def resizeEvent(self, event):
        self._width = event.size().width()
        QMainWindow.resizeEvent(self, event)

    # The application is closing. Stop some processes and storing latest options
    def closeEvent(self, event):
        self.timer.stop()
        if self.worker:
            self.worker.stop()

        self.appInfo['ShowRapid'] = self.show_rapid
        self.appInfo['SessionType'] = self.session_type
        self.appInfo['ManualRefresh'] = self.manual_mode
        self.appInfo['RefreshRate'] = self.timer.rate / 60
        self.appInfo['Position'] = [self.pos().x(), self.pos().y()]
        self.appInfo['Size'] = [self._width, self._height]

        save_app_info(ANdash, self.appInfo)

        event.accept()

    def request_information(self, is_auto, reset=False):
        if (is_auto and self.manual_mode) or self.processing.is_set():
            return
        self.processing.set()
        self.last_updated.setText('Updating...')
        self.last_updated.setStyleSheet('color: red')

        QApplication.setOverrideCursor(QCursor(Qt.WaitCursor))

        if self.manual_mode:
            self.manual.setEnabled(False)

        if reset or not (item := self.sessions.itemAtPosition(3,3)) or not item.widget().text():
            last = datetime.now() - timedelta(days=30)
        else:
            last = datetime.strptime(item.widget().text(), '%Y-%m-%d %H:%M')

        # Execute
        self.threadpool = QThreadPool()
        self.worker = FindData(self.get_sessions, last)
        self.worker.signals.result.connect(self.sessions_updated)
        self.threadpool.start(self.worker)

    # Function called after thread requestion information is finished
    def sessions_updated(self, sessions):
        nbr_ses, nbr_row = len(sessions), self.sessions.rowCount() - 3  # remove headers and separator

        if nbr_ses < nbr_row:
            self.remove_session_rows(nbr_row - nbr_ses)
        for row, ses in enumerate(sessions, 3):
            self.refresh_session_row(row, ses)

        self.last_updated.setText(datetime.now().strftime('Last updated %H:%M:%S'))
        self.last_updated.setStyleSheet('color: vlack')
        if self.manual_mode:
            self.manual.setEnabled(True)

        QTimer.singleShot(500, self.resize_it)
        QTimer.singleShot(1000, self.resize_it)

        self.processing.clear()
        QApplication.restoreOverrideCursor()

    def get_sessions(self, last):
        t1 = datetime.now()
        print('Started', t1)
        t2s = lambda t: t.strftime('%Y-%m-%d %H:%M')
        db_url = app.load_control_file(name=app.ControlFiles.Database)[-1]['Credentials'][app.args.db]
        db_tunnel = app.tunnel(app.args.db)
        sessions = {}
        yesterday = (datetime.now() - timedelta(days=7)).date()
        with IVSdata(db_url, db_tunnel) as db:
            for rec in db.orm_ses.query(models.CorrFiles).filter(models.CorrFiles.updated > last).all():
                if self.worker and self.worker.stopped.is_set():
                    return []
                if (ses_id := db.get_db_session_code(rec.code)) and (session := db.get_session(ses_id)):
                    if self.session_type != 'all' and session.type != self.session_type:
                        continue
                    downloaded = rec.updated
                    code = rec.code[:9]
                    if code in sessions:
                        continue
                    ses = [(code, False), (session.code, False), (t2s(session.start), False), (t2s(rec.updated), False)]
                    sessions[code] = ses
                    vgosdb = VGOSdb(session.db_folder)
                    if wrapper := vgosdb.get_first_wrapper('GSFC'):
                        # Get time of vgosDbCalc (first action after download)
                        if calc := wrapper.processes.get('vgosDbCalc', None):
                            downloaded = calc['runtimetag'].astimezone(get_localzone()).replace(tzinfo=None)
                            ses[3] = (t2s(downloaded), False)
                    if wrapper := vgosdb.get_last_wrapper('GSFC'):
                        if nuSolve := wrapper.processes.get('nuSolve', None):
                            analyzed = self.get_analyzed_time(vgosdb, nuSolve)
                            ses.append((t2s(analyzed), False))
                            ok, found = self.submitted(db, session, analyzed)
                            ses.extend(found)
                            if ok and downloaded.date() < yesterday:
                                sessions.pop(code)
                            continue
                    ses.extend([('Not analyzed', True), ('', True), ('', True)])

            lst = sorted(sessions.values(), key=lambda rec: rec[3][0])
            if self.show_rapid:
                lst.extend(self.get_observed_rapid(db, sessions))
            print('Exceited in', datetime.now()-t1)
            return lst

    # Read analyzed time from history file
    def get_analyzed_time(self, vgosdb, nuSolve):
        path = os.path.join(vgosdb.folder, 'History', nuSolve['history'])
        timetags = []
        with open(path) as hist:
            for line in hist:
                if line.lstrip().startswith('TIMETAG'):
                    timetags.append(self.decode_timetag(line).astimezone(get_localzone()).replace(tzinfo=None))
        timetags.sort()
        return timetags[-1]

    # Extract submitted time from cddis data files
    def submitted(self, dbase, session, analyzed):
        #tlt = lambda t: datetime.fromtimestamp(t).astimezone(get_localzone()).replace(tzinfo=None) if t else None
        tlt = lambda t: UTC.localize(t).astimezone(get_localzone()).replace(tzinfo=None) if t else None
        t2s = lambda t: t.strftime('%Y-%m-%d %H:%M')
        is_IVS = session.analysis_center.upper() == 'NASA'
        ok = [False, not is_IVS]
        found = [('unknown', True), ('unknown', True) if is_IVS else ('not IVS', False)]
        pattern = f'{"IVS" if is_IVS else "NASA"}-analysis-report'
        if reports := sorted([name for name in os.listdir(session.folder) if pattern in name]):
            ftime = tlt(info.timestamp) if (info := dbase.get(models.RecentFile, code=reports[-1])) else None
            print('Report', reports[-1], info.timestamp, ftime)
            #rpath = os.path.join(server.root, self.aux_folder, session.year, session.code, reports[-1])
            #ftime = ftime if (ftime := tlt(server.get_timestamp(rpath))) and (ftime > analyzed) else None
            ok[0], found[0] = (True, (t2s(ftime), False)) if ftime else (False, ('not on cddis', True))
        if is_IVS:
            ftime = tlt(info.timestamp) if (info := dbase.get(models.RecentFile, code=session.db_name + '.tgz')) else None
            print('VGOSdb', session.db_name+'.tgz', info.timestamp, ftime)
            #rpath = os.path.join(server.root, self.vgos_folder, session.year, session.db_name+'.tgz')
            #ftime = ftime if (ftime := tlt(server.get_timestamp(rpath))) and (ftime > analyzed) else None
            ok[1], found[1] = (True, (t2s(ftime), False)) if ftime else (False, ('not on cddis', True))

            print(found)

        return all(ok), found

    # Get list of rapid sessions that have not been correlated yet.
    def get_observed_rapid(self, dbase, sessions):
        now = datetime.now()
        start = now - timedelta(days=30)
        t2s = lambda t: t.strftime('%Y-%m-%d %H:%M')
        waiting = []
        master = ['intensive', 'standard'] if self.session_type == 'all' else [self.session_type]
        for ses_id in dbase.get_sessions(start.date(), now.date(), master):
            if session := dbase.get_session(ses_id):
                if session.db_name in sessions or session.end > now:
                    continue
                if session.type == 'standard' and not session.code.startswith(('r1', 'r4')):
                    continue
                if not os.path.exists(session.db_folder):
                    days = (datetime.utcnow() - session.end).days
                    ses = [(session.db_name, False), (session.code, False), (t2s(session.start), False),
                           (f'waiting {days:2d}d', True), ('', False), ('', False), ('', False)]
                    waiting.append(ses)

        return sorted(waiting, key=lambda rec: rec[3][0], reverse=True)

    def decode_timetag(self, line):
        # Sometime the datetime.strptime failed because of seconds = 60 or hours = 24.
        data = line.replace('TIMETAG', '').replace('UTC', '').strip()
        hour, minute, second = list(map(int, data[-8:].split(':')))
        seconds = second + minute * 60 + hour * 3600
        utc = datetime.strptime(data[:10], '%Y/%m/%d') + timedelta(seconds=seconds)
        return UTC.localize(utc)

    def exec(self):
        sys.exit(self.app.exec_())


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Generate NVI weekly or monthly report' )
    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)

    args = app.init(parser.parse_args())

    since = datetime.now() - timedelta(days=7)

    status = ANdash()
    status.exec()





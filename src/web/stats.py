import AdvancedHTMLParser
from AdvancedHTMLParser import AdvancedTag
from datetime import datetime, timedelta
from utils.servers import get_server
import tarfile
import tempfile
import os
from pathlib import Path


class STATIONstats:
    def __init__(self, template):
        # Parse input file
        with open(template) as f:
            data = f.read()

        self.parser = AdvancedHTMLParser.AdvancedHTMLParser()
        self.parser.parseStr(data)

        self.table = self.parser.getElementsByTagName('table')[0]

    def add_date(self, timestamp):
        col = AdvancedTag('td', [('class', 'start')])
        timestamp = timestamp if isinstance(timestamp, datetime) else datetime.utcfromtimestamp(timestamp)
        col.appendText(timestamp.strftime('%Y-%m-%d %H:%M'))
        return col

    def add_name(self, cls, name, link):
        col = AdvancedTag('td', [('class', cls)])
        ref = AdvancedTag('a', [('href',link)])
        ref.appendText(name)
        col.appendChild(ref)
        return col

    def add_wrappers(self, wrappers):
        col = AdvancedTag('td', [('class', 'wrappers')])
        ul = AdvancedTag('ul')
        for wrapper in wrappers:
            li = AdvancedTag('li', [('class','wrapper-id')])
            li.appendText(wrapper)
            ul.appendChild(li)
        col.appendChild(ul)
        return col

    def add(self, db_name, url, name, link, timestamp, wrappers):
        row = AdvancedTag('tr', [('class', 'session scheduled observed correlated analyzed')])
        row.appendChild(self.add_date(timestamp));
        row.appendChild(self.add_name('db_name', db_name, url))
        row.appendChild(self.add_name('name', name, link))
        row.appendChild(self.add_wrappers(wrappers))
        self.table.appendChild(row)

    def make_html(self):
        footer = self.parser.getElementsByTagName('footer')[0]
        footer.appendChild(AdvancedTag('br', isSelfClosing=True))
        footer.appendText('This page was last updated')
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')
        time = AdvancedTag('time', [('datetime',now)])
        time.appendText(now)
        footer.append(time)

        return self.parser.getFormattedHTML('\t')

    # Update page with list of recent vgosDB
    def make(self, db, rfolder):
        lfolder = tempfile.gettempdir()

        # Find latest files since last 2 weeks
        date = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')

        try:  # Create web page using template
            centers = ['cddis', 'bkg', 'opar']
            for file in db.get_recent_files('[0-9]{2}[A-Z]{3}[0-9]{2}[A-Z]{2}[.]', date):
                wrappers = []
                year = int(file.code[0:2])
                year = year + 2000 if year < 50 else 1900;
                rpath = os.path.join(rfolder, str(year), file.code)
                lpath = os.path.join(lfolder, 'web_' + file.code)
                db_name = Path(file.code).stem
                name = db.get_db_session_code(db_name)
                if not name:
                    continue
                link = os.path.join('/sessions', str(year), name)

                for center in centers:
                    with get_server(center) as server:
                        ok, err = server.download(rpath, lpath)
                        if ok:
                            url = server.url + rpath
                            break

                if ok:
                    with tarfile.open(lpath) as tar:
                        for member in tar.getmembers():
                            if member.name.endswith('.wrp'):
                                info = member.name.split('_')
                                if info[1][0] == 'V' and info[2][0] == 'i':
                                    wrappers.append('{}({})'.format(info[1], info[2][1:]))
                    wrappers.sort()

                    self.add(db_name, url, name, link, file.timestamp, wrappers)
                    os.remove(lpath)

            return ok, self.make_html()
        except Exception as err:
            return False, str(err)





import sqlite3
from datetime import datetime


class WEBdb:

    def __init__(self, db_path):
        self.con = sqlite3.connect(db_path)
        self.cur = self.con.cursor()

    def get_codes(self, table):
        self.cur.execute("SELECT code FROM {}".format(table))
        return [code[0].casefold() for code in self.cur.fetchall()]

    def update_ns(self, code, name, domes, cdp, notes):
        sql = "UPDATE vlbi_sessions_station SET name = ?, domes = ?, cdp = ?, notes = ? WHERE code = ?"
        self.cur.execute(sql, (name, domes, cdp, notes, code))

    def insert_ns(self, code, name, domes, cdp, notes):
        sql = "INSERT INTO vlbi_sessions_station " \
              "(code, name, operational, domes, cdp, notes, created_at, updated_at, full_name) " \
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        now = str(datetime.now())
        self.cur.execute(sql, (code, name, '1', domes, cdp, notes, now, now, ''))

    def insert_oper_center(self, code, name):
        sql = "INSERT INTO vlbi_sessions_operationscenter (code, name, description, notes, created_at, updated_at) " \
              "VALUES (?, ?, ?, ?, ?, ?)"
        now = str(datetime.now())
        self.cur.execute(sql, (code, name, '', '', now, now))

    def insert_corr_center(self, code, name):
        sql = "INSERT INTO vlbi_sessions_correlator (code, name, description, notes, created_at, updated_at) " \
              "VALUES (?, ?, ?, ?, ?, ?)"
        now = str(datetime.now())
        self.cur.execute(sql, (code, name, '', '', now, now))

    def insert_anal_center(self, code, name):
        sql = "INSERT INTO vlbi_sessions_analysiscenter (code, name, notes, created_at, updated_at) " \
              "VALUES (?, ?, ?, ?, ?)"
        now = str(datetime.now())
        self.cur.execute(sql, (code, name, '', now, now))

    def ns_codes(self, path):
        codes = self.get_codes('vlbi_sessions_station')
        try:
            with open(path) as file:
                for line in file.readlines():
                    if line and line[0] == ' ':
                        code, name, domes, cdp = line[1:27].split()
                        code, notes = code.casefold(), line[27:]
                        if code not in codes:
                            self.insert_ns(code, name, domes, cdp, notes)
                        else:
                            self.update_ns(code, name, domes, cdp, notes)
            self.con.commit()
            return True, ''
        except Exception as e:
            self.con.rollback()
            return False, str(e)

    def master_format(self, path):
        tables = {'SKED CODES': {'table': 'vlbi_sessions_operationscenter', 'insert': self.insert_oper_center},
                  'CORR CODES': {'table': 'vlbi_sessions_correlator', 'insert': self.insert_corr_center},
                  'SUBM CODES': {'table': 'vlbi_sessions_analysiscenter', 'insert': self.insert_anal_center}}

        category = None
        try:
            with open(path) as file:
                for line in file.readlines():
                    line = line.strip()
                    if line in tables:
                        category, codes = line, self.get_codes(tables[line]['table'])
                    elif line.startswith('end'):
                        category, codes = None, []
                    elif category:
                        if (code := line.split()[0].strip().lower()) and code not in codes:
                            tables[category]['insert'](code, line.replace(code, '').strip())
            self.con.commit()
            return True, ''
        except Exception as e:
            self.con.rollback()
            return False, str(e)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Update webdb manually')

    parser.add_argument('-n', '--ns_codes', required=False)
    parser.add_argument('-f', '--master_format', required=False)

    args = parser.parse_args()

    db = WEBdb('/sgpvlbi/progs/web/ivs-sessions/db.sqlite3')
    if args.ns_codes:
        ok, err = db.ns_codes(args.ns_codes)
    elif args.master_format:
        ok, err = db.master_format(args.master_format)
    print(ok, err)



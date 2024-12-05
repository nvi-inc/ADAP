import re
import os
import traceback
from datetime import datetime

from ivsdb.models import OperationsCenter, Correlator, AnalysisCenter, Station, Session, SessionStation
from utils import utctime, app, to_float


# def load codes from master-format file
def load_master_format(dbase, path):
    classes = {'SKED CODES': OperationsCenter, 'CORR CODES': Correlator, 'SUBM CODES': AnalysisCenter}

    category = cls = None
    try:
        with open(path, 'r') as file:
            for line in file.readlines():
                if (line := line.strip()) in classes:
                    category, cls = line, classes[line]
                elif line.startswith('end'):
                    category = cls = None
                elif category:
                    code = line.split()[0].strip()
                    agency = dbase.get_or_create(cls, code=code)
                    agency.name = line.replace(code, '').strip()

        dbase.commit()
        app.notify('DB updated', os.path.basename(path))
        return True
    except Exception as e:
        dbase.rollback()
        app.notify('DB update failed', f'{os.path.basename(path)}\n{str(e)}\n{traceback.format_exc()}')
        return False


# Load ns-codes.txt file
def load_ns_codes(dbase, path):
    try:
        with open(path, 'r') as file:
            for line in file.readlines():
                if line[0] != '*':
                    fields = [field.strip('-') for field in line.strip().split()]
                    stn = dbase.get_or_create(Station, code=fields[0].casefold())
                    stn.name, stn.domes, stn.cdp = fields[1:4]
                    stn.description = ' '.join(fields[4:])
        dbase.commit()
        app.notify('DB updated', os.path.basename(path))
        return True
    except Exception as e:
        dbase.rollback()
        app.notify('DB update failed', f'{os.path.basename(path)}\n{str(e)}\n{traceback.format_exc()}')
        return False


COLUMNS = {'1.0': ['name', 'code', 'date', 'DOY', 'time', 'dur', 'stations', 'sked', 'corr',
                   'status', 'PF', 'DBC', 'subm', 'DEL', 'MK4'],
           '2.0': ['name', 'date', 'code', 'DOY', 'time', 'dur', 'stations', 'sked', 'corr',
                   'status', 'DBC', 'subm', 'DEL']
           }


def decode_duration(text):
    try:
        hours, minutes = text.split(':')
        return to_float(hours) * 3600 + to_float(minutes) * 60
    except ValueError:
        return to_float(text, '86400')


def decode_start(version, year, record):
    if version == '2.0':
        start = f'{record["date"]} {record["time"]}'
        return datetime.strptime(start, '%Y%m%d %H:%M')
    else:
        start = f'{year} {record["date"]} {record.get("time", "00:00").replace("24:", "00:")}'
        return utctime.utc(start, '%Y %b%d %H:%M')


# Read master file and store session information in database.
# Some checks are not done because file has already been validated
def parse_master(dbase, path):
    types = app.load_control_file(name=app.ControlFiles.Types)[-1]
    types = {ses_id.upper(): ses_type.upper() for ses_type, sessions in types.items() for ses_id in sessions}

    warnings = []
    # Read data from master file
    with open(path, 'r') as mst:
        mst_content = mst.read()
    # Find year from title
    year = re.findall(r'(\d{4}) MULTI-AGENCY.+SCHEDULE', mst_content)[0]
    # Extract header, data and footnote (_)
    header, *lines, _ = mst_content.splitlines()
    # Get version from header
    version = re.match(r'## Master file format version (?P<version>\d\.\d).*', header)["version"]
    # Extract type from path and create old and new name
    ses_type = {'-int': 'intensive', '-vgos': 'vgos'}.get(
        re.match(r'master(\d*)(?P<type>(|-int|-vgos))?\.txt', os.path.basename(path))['type'], 'standard')
    # Delete all records from old master file
    for (ses_id, _) in dbase.get_sessions_from_year(year, [ses_type]):
        dbase.delete(dbase.get_session(ses_id))
    dbase.flush()
    # Extract session information. Use new master name for database
    for data in [line.strip().split('|')[1:-1] for line in lines if line.startswith('|')]:
        record = dict(**{name: val.strip().lower() for name, val in zip(COLUMNS.get(version), data)})
        # Sometimes codes have not been created in other tables
        if not dbase.get(Correlator, code=record['corr'].casefold()):
            dbase.add(Correlator(record['corr'].casefold()))
            warnings.append(f'{record["corr"]} added to Correlators')
        if not dbase.get(OperationsCenter, code=record['sked'].casefold()):
            dbase.add(OperationsCenter(record['sked'].casefold()))
            warnings.append(f'{record["sked"]} added to Operations Centers')
        if not dbase.get(AnalysisCenter, code=record['subm'].casefold()):
            dbase.add(AnalysisCenter(record['subm'].casefold()))
            warnings.append(f'{record["subm"]} added to Analysis Centers')
        included, removed = [list(re.findall('..', grp)) for grp in (record['stations'].split(' -')+[''])[:2]]
        for code in included + removed:
            if not dbase.get(Station, code=code.casefold()):
                dbase.add(Station(code.casefold()))
                warnings.append(f'{code} added to Network Stations')
        dbase.flush()
        # Add record to database
        session = dbase.get_or_create(Session, code=record['code'], )
        session.corr_status = record['status']
        session.start = decode_start(version, year, record)
        session.duration = decode_duration(record['dur'])
        session.name = types.get(session.code.upper(), record['name'])
        session.type = ses_type

        session.correlator, session.operations_center = record['corr'].casefold(), record['sked'].casefold()
        session.analysis_center = record['subm'].casefold()
        session.corr_db_code = record['DBC']

        # Add SessionStation records
        for status, grp in {'included': included, 'removed': removed}.items():
            for sta in grp:
                if not (ses_sta := dbase.get(SessionStation, session=session.code, station=sta.casefold())):
                    ses_sta = SessionStation(session.code, sta.casefold())
                ses_sta.status = status
                session.participating.append(ses_sta)
    return warnings


def load_master(dbase, path):

    try:
        # Open database
        warnings = parse_master(dbase, path)
        dbase.commit()
        warnings, nl = ('\n'.join(warnings), '\n') if warnings else ('', '')
        app.notify('DB updated', f'{os.path.basename(path)}{nl}{warnings}')
        return True
    except Exception as exc:
        dbase.rollback()
        app.notify('DB update failed', f'{os.path.basename(path)}\n{str(exc)}\n{traceback.format_exc()}')
        return False

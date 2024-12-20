import re
from datetime import datetime
import gzip

from utils import app, to_float
from ivsdb import IVSdata
from ivsdb.models import SessionStation, SEFD, Detector


def save_sefd(csv, station, data):
    if not data:
        return False
    try:
        common_data = data[0]
        source, observed = common_data['source'], common_data['time']

        sefd = SEFD(station=station, source=source, observed=observed)
        sefd.station, sefd.source, sefd.observed = station, source, observed
        sefd.azimuth = common_data['Az']
        sefd.elevation = common_data['El']

        for value in data:
            detector = Detector()
            detector.id = sefd.id
            detector.device = value['De']
            detector.input = int(value['I'])
            detector.frequency = to_float(value['Center'])
            detector.polarization = value['P']
            detector.gain_compression = to_float(value['Comp'])
            detector.tsys = to_float(value['Tsys'])
            detector.sefd = to_float(value['SEFD'])
            detector.tcal_j = to_float(value['Tcal(j)'])
            detector.tcal_r = to_float(value['Tcal(r)'])
            sefd.detectors.append(detector)
        print(sefd.to_csv(), file=csv)
        return True
    except:
        return False


def get_onoff(csv, sta_id, path):
    is_header = re.compile(r'^(?P<time>^\d{4}\.\d{3}\.\d{2}:\d{2}:\d{2}\.\d{2})(?P<key>#onoff#    source)'
                           r'(?P<data>.*)$').match
    is_onoff = re.compile(r'^(?P<time>^\d{4}\.\d{3}\.\d{2}:\d{2}:\d{2}\.\d{2})(?P<key>#onoff#VAL)'
                          r'(?P<data>.*)$').match

    nbr, header, records = 0, [], []
    with open(path, 'r', encoding="utf8", errors="ignore") as f:
        for line in f:
            if found := is_onoff(line):
                timestamp = datetime.strptime(found['time'], '%Y.%j.%H:%M:%S.%f')
                record = {name: value for name, value in zip(header, found['data'].split())}
                records.append(dict(**{'time': timestamp}, **record))
            elif found := is_header(line):
                header = ['source'] + found['data'].split()
                nbr += save_sefd(csv, sta_id, records)  # Send existing onoff records to VCC
                records = []

        nbr += save_sefd(csv, sta_id, records)
        return nbr


if __name__ == '__main__':
    import argparse
    from pathlib import Path
    import os

    from utils import readDICT
    from utils.mail import send_message, build_message

    parser = argparse.ArgumentParser(description='Extract SEFDs from logs')
    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)

    app.init(parser.parse_args())

    url, tunnel = app.get_dbase_info()
    vgos = dict(Gs=0, K2=0, Mg=0, Nn=0, Oe=0, Ow=0, Sa=0, Wf=0, Ws=0, Yj=0)
    mixed = dict(Hb=0, Is=0)

    today = datetime.now().strftime("%Y-%m-%d")
    name = f'/tmp/sefd-{today}.csv.gz'
    with IVSdata(url, tunnel) as dbase, gzip.open(name, 'wt') as csv:
        for sta_id, nbr in vgos.items():
            for (ses_id,) in dbase.orm_ses.query(SessionStation.session).filter(SessionStation.station == sta_id).all():
                if (session := dbase.get_session(ses_id)) and (log := session.log_path(sta_id)).exists():
                    nbr += get_onoff(csv, sta_id, log)
            vgos[sta_id] = nbr

    # Email file
    message = f'VGOS stations  {",".join([sta_id for sta_id, nbr in vgos.items()])}'
    details = readDICT(Path(os.environ['CONFIG_DIR'], 'sefd.toml'))
    msg = build_message(details['sender'], details['recipients'], f'SEFDs for vgos stations {today}',
                        reply=details['reply'], text=message, files=[str(name)])
    send_message(details['server'], msg)

import os
import sys
import glob

from utils import app
from utils.servers import load_servers, get_server, DATACENTER
from tools import record_submitted_files
from ivsdb.models import UploadedFile


# Make full list of files and submit to selected center
def upload_files(center, files):
    # Get server for this data center
    if center not in load_servers(DATACENTER):
        print(f'No information for {center} in list of servers')
        sys.exit(1)

    server = get_server(DATACENTER, center)

    user = os.environ['SUDO_USER'] if 'SUDO_USER' in os.environ else os.environ['USER']
    app_name = f'upload_{center}'

    submitted = []
    for pattern in files:
        for name in server.upload([file for file in glob.glob(pattern)]):
            print(f'{name} was uploaded to {center}')
            submitted.append(UploadedFile(name, user, app_name, 'ok'))

    if submitted:
        record_submitted_files(submitted)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Upload files to any IVS Data Center.' )
    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('center', help='IVS Data Center', choices=['bkg', 'cddis', 'opar'])
    parser.add_argument('files', nargs='+')

    args = app.init(parser.parse_args())

    upload_files(args.center, args.files)



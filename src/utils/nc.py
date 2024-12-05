from pathlib import Path
import re

from netCDF4 import Dataset, stringtochar
import numpy as np

do_not_check = set(['CreateTime', 'CreatedBy'])


def same(first, second):
    # Make sure both files exists
    if not (first.exists() and second.exists()):
        return False
    with Dataset(first) as nc1, Dataset(second) as nc2:
        keys_1, keys_2 = sorted(list(nc1.variables.keys())), sorted(list(nc2.variables.keys()))
        if keys_1 != keys_2:
            return False
        for key in keys_1:
            if key not in do_not_check and not np.array_equal(nc1.variables[key][:], nc2.variables[key][:]):
                return False
    return True


def update_create_time(path, utc):
    with Dataset(path, mode='r+') as f:
        f.variables['CreateTime'][:] = stringtochar(np.array([utc.strftime('%Y/%m/%d %H:%M:%S UTC')], 'S'))


def version(path):
    match = re.match(r'(?P<name>.*)(_V(?P<version>\d{3}).nc)|.nc', str(path))
    return int(match['version']) if match['version'] else 0


def rename(path, version):
    if version > 0:
        return path
    name = re.match(r'(?P<name>.*)(_V(?P<version>\d{3}).nc)|.nc', str(path))['name']
    return f"{name}_V{version:03d}.nc"


if __name__ == '__main__':
    import argparse
    from datetime import datetime

    parser = argparse.ArgumentParser(description='NC file utility')

    parser.add_argument('-s', '--same', help='test if 2 files are same', nargs=2, required=False)
    parser.add_argument('-u', '--update', help='update create time', nargs='+', required=False)

    args = parser.parse_args()

    if args.same:
        print(same(Path(args.same[0]), Path(args.same[1])))
    elif args.update:
        path, utc = args.update if len(args.update) == 2 else (args.update[0], 'now')
        utc = datetime.utcnow() if utc == 'now' else datetime.fromisoformat(utc)
        update_create_time(path, utc)

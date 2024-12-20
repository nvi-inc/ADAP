import os
from pathlib import Path
import shutil
import re

from netCDF4 import Dataset, stringtochar
import numpy as np


# Custom Exception for reporting file and problem
class Problem(Exception):
    def __init__(self, err_msg):
        self.err_msg = err_msg

    def __str__(self):
        return self.err_msg


# Replace code in file since it has same length than old code.
def replace_session_code(path, code):
    modified = path.stat().st_mtime
    with Dataset(path, 'r+', clobber=True) as nc:
        if 'Session' not in nc.variables:
            return False
        try:
            nc.variables['Session'][:] = stringtochar(np.array([code], 'S'))
        except ValueError as err:
            raise Problem(f'Could not update "Session" variable in {str(path)} : {str(err)}')
    os.utime(path, (modified, modified))
    return True


# Need to copy all information and change the dimension of Session variable
def fix_session_code(path, code):
    new_path = Path(str(path)+'_')
    modified = path.stat().st_mtime
    with Dataset(path) as src:
        if 'Session' not in src.variables:
            return False
        # Find name of dimension and set value of string
        var_len = 'SessionLen'
        value = stringtochar(np.array([code], 'S'))
        with Dataset(new_path, mode='w', format=src.data_model) as trg:
            # Create the dimensions of the file
            try:
                trg.createDimension(var_len, len(code))
                for name, dim in src.dimensions.items():
                    if name != var_len:
                        trg.createDimension(name, len(dim) if not dim.isunlimited() else None)
            except ValueError as err:
                raise Problem(f'Could not create dimension: {str(err)}')
            # Copy the global attributes
            try:
                trg.setncatts({a: src.getncattr(a) for a in src.ncattrs()})
            except ValueError as err:
                raise Problem(f'Could not copy global attributes in {str(path)}: {str(err)}')
            # Create the variables in the file
            for name, var in src.variables.items():
                try:
                    trg.createVariable(name, var.dtype, var_len if name == 'Session' else var.dimensions)
                    # Copy the variable attributes
                    trg.variables[name].setncatts({a: var.getncattr(a) for a in var.ncattrs()})
                    # Copy the variables values
                    trg.variables[name][:] = value if name == 'Session' else src.variables[name][:]
                except ValueError as err:
                    raise Problem(f'Could not create {name} variable in {str(path)}: {str(err)}')

    # Set same modified time for new file and overwrite old file
    shutil.move(new_path, path)
    os.utime(path, (modified, modified))
    return True


# Replace session code in text file. Re-write on same file
def replace_text(path, old_code, new_code):
    modified = path.stat().st_mtime
    with open(path, "r+") as f:
        if found := new_code in (content := re.sub(old_code, new_code, f.read(), re.IGNORECASE)):
            f.seek(0)
            f.truncate()
            f.write(content)
    os.utime(path, (modified, modified))
    return found


# Control the modifications in all vgosdb folder.
def fix_vgosdb_code(path, code):
    # Get old code in database.
    if not (head := Path(path, 'Head.nc')).exists():
        print(f'{str(head)} does not exist!')
        return
    with Dataset(head) as nc:
        old_code = nc.variables['Session'][:].tobytes().decode('utf-8')
    # Decide which function used to repair Session
    fnc = replace_session_code if len(old_code) == len(code) else fix_session_code
    try:
        for file in path.glob('**/*.*'):
            if file.is_file():
                fnc(file, code) if file.suffix == '.nc' else replace_text(file, old_code, code)
    except Problem as err:
        print(err)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Change Session code in vgosDB')

    parser.add_argument('path', help='path of vgosdb folder')
    parser.add_argument('code', help='session code', type=str.upper)

    args = parser.parse_args()

    fix_vgosdb_code(Path(args.path), args.code)

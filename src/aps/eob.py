import math
import os
import re

from pathlib import Path

from utils import app
from aps.leap_seconds import get_UTC_minus_TAI
from aps.astro import MSEC__TO__RAD, OM__EAR


# Dictionary describing each field in eob record (See getpar_02.txt section 3.10).
eopb = dict([('MJD_EOP', 'F:0'), ('SCODE', 'A:2'), ('XPL_V', 'F:3'),
             ('YPL_V', 'F:4'), ('U1_V', 'F:5'), ('DPSI_V', 'F:6'), ('DEPS_V', 'F:7'), ('XPR_V', 'F:8'),
             ('YPR_V', 'F:9'), ('UTR_V', 'F:10'), ('XPL_E', 'F:11'), ('YPL_E', 'F:12'), ('U1_E', 'F:13'),
             ('DPSI_E', 'F:14'), ('DEPS_E', 'F:15'), ('XPR_E', 'F:16'), ('YPR_E', 'F:17'), ('UTR_E', 'F:18'),
             ('CXY', 'F:19'), ('CXU', 'F:20'), ('CYU', 'F:21'), ('CPE', 'F:22'), ('CURX', 'F:23'),
             ('CURY', 'F:24'),
             ('CURU', 'F:25'), ('DURA', 'F:26'), ('WRMS', 'F:27'), ('NOBS', 'I:28'), ('MJD_NUT', 'F:29'),
             ('NET', 'A:30')
             ])
scode_col, xpl_col = int(eopb['SCODE'].split(':')[-1]), int(eopb['XPL_V'].split(':')[-1])
nan = float('nan')
# Decode eob values using special functions
decoders = {'F': lambda s: nan if s in ('-0', '****', 'NA') else float(s.strip()),
            'I': lambda s: int(s.strip()),
            'A': lambda s: s.strip(),
            'C': lambda s: s
            }


# Extract eob data from line
def get_eob_data(line, is_eopm):
    def decode(fmt):
        code, index = fmt.split(':')
        return decoders[code](data[int(index)])

    data = line[1:].split()
    data[scode_col] = '' if data[scode_col] == data[xpl_col] else data[scode_col]
    decoded = {key: decode(fmt) for key, fmt in eopb.items()}
    #if is_eopm:
    #    for key in ('CXY', 'CXU', 'CYU', 'CPE'):
    #        decoded[key] = nan
    return decoded


# List describing order and format of output (see eops_format.txt)
eops = [('MJD_EOP', 'F12.6'), ('XPL_V', 'F8.6'), ('YPL_V', 'F8.6'), (('U1_V', 'u1v'), 'F10.7'),
        ('DPSI_V', 'F8.3'), ('DEPS_V', 'F8.3'), ('XPL_E', 'F8.6'), ('YPL_E', 'F8.6'), ('U1_E', 'F9.7'),
        ('DPSI_E', 'F7.3'), ('DEPS_E', 'F7.3'), ('WRMS', 'F7.2'), ('CXY', 'F6.4'), ('CXU', 'F6.4'), ('CYU', 'F6.4'),
        ('CPE', 'F6.4'), ('NOBS', 'I6'), ('SCODE', 'A6'), ('DURA', 'F5.2'), ('XPR_V', 'F9.6'), ('YPR_V', 'F9.6'),
        (('UTR_V', 'utrv'), 'F10.7'), (None, 'NA'), (None, 'NA'), ('XPR_E', 'F9.6'), ('YPR_E', 'F9.6'),
        (('UTR_E', 'utrv'), 'F10.7'), (None, 'NA'), (None, 'NA'), (None, ''), ('NET', 'A64')
        ]
# Transform eops values using special functions
transformers = {'utrv': lambda k, r: r[k] * (-MSEC__TO__RAD / OM__EAR if k == 'UTR_V' else MSEC__TO__RAD / OM__EAR),
                'u1v': lambda k, r: r[k] - get_UTC_minus_TAI(r['MJD_EOP'] + 2400000.5)
                }


# Define some functions to format eops values
def f2str(value, fmt):  # Transform the string to float if defined and format with specific precision
    length, precision = fmt[1:].split('.')
    if math.isnan(value):
        return '{:<{length}s}'.format('NA', length=length)
    string = '{:{length}.{precision}f}'.format(value, length=length, precision=precision)
    # Python output -0. values smaller than 0. For some format, the leading zero should be removed
    return string.replace('-0.', '-.') if len(string) > int(length) and string.startswith('-0.') else string


formatters = {'F': f2str,
              'A': lambda v, f: '{:<{length}s}'.format(v, length=f[1:]),
              'I': lambda v, f: '{:{length}d}'.format(v, length=f[1:])
              }


def make_eops_record(data, is_eopm=True):
    def transform(key):
        return transformers[key[1]](key[0], data) if isinstance(key, tuple) else data.get(key, None)

    def format_it(value, fmt):
        return formatters.get(fmt[0], lambda v, f: f)(value, fmt) if fmt else fmt

    # New format for network
    if '-' not in (net := data.get('NET', '')):
        data['NET'] = '-'.join(re.findall('..', net))
    return [format_it(transform(k), f) for k, f in eops]


def test_records(path, is_eopm=True):

    with open(path) as f:
        for line in f:
            if not line.startswith('#'):
                print(' '.join(make_eops_record(get_eob_data(line, is_eopm))))


def eob_to_eops(path_eob, path_eops, is_eopm=True):
    # Read eops_format.txt file
    root, path = app.Applications.APS['Files']['HelpEOPS']
    #read constrains from path_cmt
    with open(path_eob, errors='ignore') as feob:
        data = [make_eops_record(get_eob_data(line, is_eopm)) for line in feob if not line.startswith('#')]
    start, end = data[0][0], data[-1][0]
    print(start, end, len(data))

    with open(path_eops, 'w') as feops:
        #Write header
        print('+DATA', file=feops)
        #Write 2 lines
        for record in data:
            print(' '.join(record), file=feops)
        print('-DATA', file=feops)
        #Write footer



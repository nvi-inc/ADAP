from pathlib import Path

from utils import app, to_float

UT1LS = None


# Setup so it read only once
def read_ut1ls():
    global UT1LS

    if not UT1LS:
        UT1LS = {'first': None, 'last': None, 'data': [], 'modified': 0}
    if not (path := Path(app.Applications.APS['Files']['LeapSeconds'])).exists():
        raise Exception(f'{str(path)} does not exist!')
    if (modified := path.stat().st_mtime) != UT1LS['modified']:
        UT1LS['modified'] = modified
        try:
            with open(path) as ls:
                for line in ls:
                    JD, TAImUTC = to_float(line[17:26]), to_float(line[36:48])
                    if not UT1LS['first']:
                        UT1LS['first'] = JD
                    UT1LS['last'] = JD
                    UT1LS['data'].append((JD, TAImUTC))
        except Exception as err:
            raise Exception(f'Problem reading leap seconds file [{str(err)}]')


# Get UT1 - TAI for specific julian date
def get_UTC_minus_TAI(julian_date):
    read_ut1ls()
    # Test out of limit
    if julian_date < UT1LS['first'] or julian_date > UT1LS['last']:
        raise Exception(f'{julian_date} not between {UT1LS["first"]} and {UT1LS["last"]}')

    for JD, TAImUTC in UT1LS['data']:
        if JD > julian_date:
            break
        UTCmTAI = -TAImUTC
    return UTCmTAI

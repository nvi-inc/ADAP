from aps.eob import test_records

if __name__ == '__main__':
    import argparse
    from utils import app
    import os

    parser = argparse.ArgumentParser(description='Execute script updating apriori EOP files')
    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('eob')

    app.init(parser.parse_args())

    test_records(os.path.join('/sgpvlbi/level3/eo/', app.args.eob))

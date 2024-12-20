
if __name__ == '__main__':
    from ivsdb import IVSdata, loaders
    from utils import app
    import argparse
    import os
    from pathlib import Path

    parser = argparse.ArgumentParser( description='Logger for ADAP software.' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('path')

    app.init(parser.parse_args())
    db_url, db_tunnel = app.get_dbase_info()
    with IVSdata(db_url, db_tunnel) as dbase:
        path = Path(app.VLBIfolders.control, app.args.path)
        print(str(path), loaders.load_master(dbase, path))


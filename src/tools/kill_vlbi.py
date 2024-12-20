from pathlib import Path
from psutil import Process, process_iter

from utils import app


def send_stop(name):
    for prc in process_iter(attrs=['pid', 'name', 'cmdline']):
        if prc.info['name'] == 'python':
            print(prc.info['cmdline'])
            if '-q' in (cmds := prc.info['cmdline']):
                queue = cmds[cmds.index('-q')]
                app_name = Path(cmds[1]).stem
                print(f'Found', app_name, queue)


"""
            if name in prc.info['cmdline']
            name = [Path(path) for path in prc.info['cmdline'] if path.startswith('/sgpvlbi/progs/adap')]
            if name and (name := name[0].replace('.py', '')) not in ['VLBIintmonit']:
                try:
                    Process(prc.info['pid']).kill()
                    print(f'Successfully killed process {name} {prc.info["pid"]}')
                except Exception as err:
                    print(f'Failed killing process {name} {prc.info["pid"]}. [{str(err)}]')
"""

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Logger for ADAP software.' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('app', help='application name')

    args = app.init(parser.parse_args())
    send_stop(args.app)

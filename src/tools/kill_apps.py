from psutil import Process, process_iter
import os


def kill():
    for prc in process_iter(attrs=['pid', 'name', 'cmdline']):
        if prc.info['name'] == 'python':
            name = [os.path.basename(path) for path in prc.info['cmdline'] if path.startswith('/sgpvlbi/progs/adap')]
            if name and (name := name[0].replace('.py', '')) not in ['VLBIintmonit']:
                try:
                    Process(prc.info['pid']).kill()
                    print(f'Successfully killed process {name} {prc.info["pid"]}')
                except Exception as err:
                    print(f'Failed killing process {name} {prc.info["pid"]}. [{str(err)}]')


kill()

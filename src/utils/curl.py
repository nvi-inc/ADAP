from utils import app


# Check if file exists using curl
def file_exists(rpath):
    if not (ans := app.exec_and_wait(f'curl -L -I -k -s {rpath}')[0]):
        return False

    rsp, *lines = ans.splitlines()
    if not rsp.startswith('HTTP') or not rsp.endswith('200 OK'):
        return False
    try:
        header = dict(zip(*list(zip(*[line.split(':', 1) for line in lines if line]))))
        if int(header.get('Content-Length', '0')) > 0:
            return True
    except Exception as err:
        print(err)
    return False


# Download a file using curl
def download(rpath, lpath):
    if not file_exists(rpath):
        return False
    return not app.exec_and_wait(f'curl -S -s -k {rpath} --output {lpath}')[-1]


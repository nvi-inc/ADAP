import inspect
from pathlib import Path
from subprocess import Popen, PIPE
from importlib import import_module, reload
from typing import List, Tuple


def grep(folder: Path, cmd: str):
    try:
        full_cmd = f'grep -rE --include \*.py \"{cmd}\" {folder}'
        print(full_cmd)
        st_out, _ = Popen(full_cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE).communicate()
        return st_out.decode('utf-8').splitlines()
    except:
        return []


def get_module_items(path):
    items = set()
    module_name = f'{path.parent.name}.{path.stem}'
    the_module = import_module(module_name)
    for name, obj in the_module.__dict__.items():
        if not (obj_module := inspect.getmodule(obj)) or obj_module.__name__ == module_name:
            if inspect.isclass(obj) or inspect.isfunction(obj) or not name.startswith(('_', 'last_mod_time')):
                items.add(name)

    return items


def find_files(folder, path):
    package, name, cmds = path.parent.name, path.stem, []
    if path.stem == '__init__':
        cmds = [f'import {package}\\s?$', f'from {package} import.*({"|".join(get_module_items(path))})']
    elif path.stem == '__main__':
        pass
    else:
        cmds = [f'from {package} import.*{name}', f'from {package}\.{name} import']

    files = [Path(line.split(':')[0]) for cmd in cmds for line in grep(folder, cmd)]
    return sorted(set(files))


def find_apps(folder: Path, package: str, name: str) -> List[Path]:
    if not (path := Path(folder, package, f'{name}.py')).is_file():
        return []

    applications, processed, to_check = set(), set(), []
    while True:
        processed.add(path)

        for file in find_files(folder, path):
            if file.name.startswith('VLBI'):
                applications.add(file)
            elif file.parent != folder and file not in processed:
                to_check.append(file)
        if not to_check:
            break
        path = to_check.pop(0)

    print('\n'.join([str(file) for file in processed]))
    print('\n'.join([str(file) for file in applications]))
    return f'processed {len(processed)} files. Found {len(applications)} apps'


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Watchdog for ADAP software.')
    parser.add_argument('folder')
    parser.add_argument('path')

    args = parser.parse_args()
    folder = Path(args.folder)
    path = Path(folder, args.path)
    package, name = path.parent.name, path.stem
    if package == 'aps' and name != '__main__':
        name = '__init__'
    print(path, find_apps(folder, package, name))

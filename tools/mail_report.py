from datetime import datetime
from pathlib import Path

from utils import app
from utils.mail import build_message, send_message


def get_path_of_report(ans):
    for line in ans.splitlines():
        if line.startswith('Report:'):
            return line.split(':')[1].strip()
    return None


def process(name):
    # Get information from config file
    if not (config := app.load_control_file(name=app.ControlFiles.Crons)[-1].get(name, None)):
        return f'ERROR: Name [{name}] is not in {app.ControlFiles.Crons} file'

    # Execute command
    ans, err = app.exec_and_wait(config['cmd'])
    if err:
        return f'ERROR\n{err}\n\nANSWER\n{ans}'

    # Prepare message
    if name == 'Summary':
        title = f"summarize weekly output for {datetime.now().strftime('%Y-%m-%d')}"
        message, files = ans, []
    elif report := get_path_of_report(ans):
        path = Path(report)
        title = f'NVI {name} Report {path.stem}'
        message, files = 'See attached file', [report]
    else:
        return f'ERROR: Problem with {name} Report\n{ans}'

    # Build and send message
    msg = build_message(config['sender'], config['recipients'], title, text=message, files=files)
    return send_message(config['server'], msg)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Run statistics with specific scrip' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('param')

    app.init(parser.parse_args())

    if err := process(app.args.param.capitalize()):
        print(err)
        app.notify('Error in stats', err)


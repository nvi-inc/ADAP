import os
import sys
import select
from datetime import datetime

from utils.mail import build_message, send_message, get_mail_info


# Program to send email using smtp ot gmail servers
def mailit(args):
    errors = []  # Keep error messages
    # Define sender
    server, sender = args.fr, args.server
    if not all((server, sender)):
        server, sender = get_mail_info()
    if not server:
        errors.append('Server was not defined')
    if not sender:
        errors.append('Sender was not defined')
    # Check subject
    if not args.subject:
        errors.append('Subject header is missing. Use mailit -h to see usage.')
        title = 'None'
    else:
        title = ' '.join(args.subject)
    # Check recipient list
    if not args.recipients:
        errors.append('No recipient list. Use mailit -h to see usage.')
    # Read file or stdin for text in body
    if args.body:
        paragraphs = []
        for path in args.body:
            path = os.path.expanduser(path)
            if os.path.exists(path):
                paragraphs.append(open(path).read())
            else:
                errors.append(f'{path} does not exist')
        message = '\n'.join(paragraphs)
    elif select.select([sys.stdin], [], [], 0)[0]:
        message = sys.stdin.read()
    else:
        message = ''
    # Add files as attachment
    files = args.attachment if args.attachment else []
    [errors.append(f'{path} does not exist') for path in files if not os.path.exists(path)]
    if not errors:
        msg = build_message(sender, args.recipients, title, text=message, files=files)
        if err := send_message(server, msg):
            errors.append(err)
    if errors:
        fout = open(args.error, 'a+') if args.error else sys.stdout
        [print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - {title} [{err}]', file=fout) for err in errors if err]


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Send email using NASA smtp server' )
    parser.add_argument('-fr', '--fr', help='from email', required=False)
    parser.add_argument('-server', '--server', help='server', required=False)
    parser.add_argument('-a', '--attachment', help='File to be included as attachment', nargs='+', required=False)
    parser.add_argument('-s', '--subject', help='Subjet header', nargs='+', required=False)
    parser.add_argument('-r', '--recipients', help='List of recipients', nargs='+', required=False)
    parser.add_argument('-b', '--body', help='Path to text file', nargs='+', required=False)
    parser.add_argument('-e', '--error', help='Path to error log', nargs='?', required=False)

    args = parser.parse_args()

    mailit(args)

from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import message_from_bytes


class Gmail:

    def __init__(self, credentials):
        creds = file.Storage(credentials).get()
        self.service = build('gmail', 'v1', http=creds.authorize(Http()), cache_discovery=False) if creds else None
        if self.service:
            labels = self.service.users().labels().list(userId='me').get('labels', [])
            self.labels = {label['name']: label['id'] for label in labels}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def new_message(self, sender):
        # Init MIME multipart message
        self.msg = MIMEMultipart('alternative')
        self.sender = sender

    @property
    def available(self):
        return bool(self.service)

    @property
    def sender(self):
        return self.msg.get('from', '')

    @sender.setter
    def sender(self, value):
        self.msg['from'] = value

    @property
    def recipients(self):
        return self.msg.get('to', [])

    @recipients.setter
    def recipients(self, value):
        value = value if isinstance(value, list) else [value]
        self.msg['to'] = ', '.join(value)

    @property
    def reply_to(self):
        return self.msg.get('reply-to', '')

    @reply_to.setter
    def reply_to(self, value):
        self.msg['reply-to'] = value[0] if isinstance(value, list) else value

    @property
    def copy_to(self):
        return self.msg.get('cc', [])

    @copy_to.setter
    def copy_to(self, value):
        value = value if isinstance(value, list) else [value]
        self.msg['cc'] = ', '.join(value)

    @property
    def priority(self):
        return self.msg.get('X-Priority', '3') == '1'

    @priority.setter
    def priority(self, value):
        self.msg['X-Priority'], self.msg['X-MSMail-Priority'] = ('1', 'High') if value else ('3', 'Normal')

    def send(self, msg):
        try:
            body = {'raw': base64.urlsafe_b64encode(msg.as_bytes()).decode()}
            return True, (self.service.users().messages().send(userId='me', body=body).execute())
        # except errors.HttpError, error:
        except Exception as err:
            return False, str(err)

    def send_message(self, subject, text, html):
        # Make sure sender and recipients have been defined
        if not self.sender:
            return False, 'Sender not defined'
        if not self.recipients:
            return False, 'Recipients not defined'
        if not subject:
            return False, 'No subject line'

        self.msg['subject'] = subject
        self.msg.attach(MIMEText(text, 'plain'))
        self.msg.attach(MIMEText(html, 'html'))

        return self.send(self.msg)

    # Get label list:
    def get_labels(self):
        labels = self.service.users().labels().list(userId='me').get('labels', [])
        self.labels = {label['name']: label['id'] for label in labels}
        print(self.labels)

    # Get list of UIDs for a specific label
    def get_uids(self, labels, filters=None):

        # Make query in gmail format
        query = f'{" ".join(filters)}' if filters else ""
        page_token = None
        while response := self.service.users().messages().list(userId='me', q=query, labelIds=labels,
                                                               pageToken=page_token).execute():
            for msg in response.get('messages', {}):
                if uid := msg.get('id', None):
                    yield uid
            if not (page_token := response.get('nextPageToken', None)):
                break

    # Get a message using its uid
    def get_msg(self, uid):
        message = self.service.users().get(userId='me', id=uid, format='raw').execute()
        return message_from_bytes(base64.urlsafe_b64decode(message['raw'].encode('utf-8')))

    # Mark message as read and set flags if required
    def mark_as_read(self, uid, flags=[]):

        labels = {'removeLabelIds': ['UNREAD', 'INBOX'], 'addLabelIds': flags}
        self.service.users().messages().modify(userId='me', id=uid, body=labels).execute()

    # Move message to trash (cannot delete directly)
    def delete(self, uid):
        self.service.users().messages().trash(userId='me', id=uid).execute()


if __name__ == '__main__':
    from utils import app, readDICT
    import argparse

    def make_message():
        # Make html message
        html = """
        <h3>The following schedules have not been submitted to the IVS Data Center</h3>
        <table style = 'font-family: arial, sans-serif; border-collapse: collapse;'>
        <tr>
        <th style=align: left>Status</th>
        <th>Session</th>
        <th>Start time</th>
        </tr>
        <tr>
        <td style='color: #FF0000; padding: 0 10px 0 0;'>URGENT</td>
        <td style='color: #FF0000; padding: 0 20px 0 0;'><a href="https://ivscc.gsfc.nasa.gov/sessions/2020/q20095">Q20095</a> starts in 3 days</td>
        <td style='color: #FF0000;'>Saturday, April 04, 2020 at 07: 30 UTC</td>
        </tr>
        <tr>
        <td style='color: #000000; padding: 0 10px 0 0;'>LATE</td>
        <td style='color: #000000; padding: 0 20px 0 0;'><a href="https://ivscc.gsfc.nasa.gov/sessions/2020/q20096">Q20096</a> starts in 4 days</td>
        <td style='color: #000000;'>Sunday, April 05, 2020 at 18:00 UTC</td>
      </tr>
      
    </table>
    <p>For information please contact <a href='mailto:"Thomas, Cynthia C. (GSFC-61A.0)[NVI INC]"<cynthia.c.thomas@nasa.gov>'>Cynthia Thomas</a></p>
    <p>Visit <a href="https://ivscc.gsfc.nasa.gov/sessions/">IVS Sessions page</a></p>
    
    
       """

        text = """
        The following schedules have not been submitted to the IVS Data Center
    
        URGENT - Q20095 starts in 3 days Saturday, April 04, 2020 at 07: 30 UTC</td>
        LATE   - Q20096 starts in 4 days Sunday, April 05, 2020 at 18:00 UTC</td>
     
        For information please contact Cynthia Thomas at cynthia.c.thomas@nasa.gov
        Visit https://ivscc.gsfc.nasa.gov/sessions/ to see IVS Sessions page
    
        """

        return text, html

    subject = "Testing gmail account"
    text, html = make_message()

    parser = argparse.ArgumentParser( description='Gmail test.' )

    parser.add_argument('-c', '--config', help='config file', default='~/.config/adap/vlbi.toml', required=False)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-u', '--user', help='user secret credentials', default='ivscc-mail', required=False)

    args = app.init(parser.parse_args())

    credentials = app.secret('Mail', args.user)
    with Gmail(credentials) as gmail:
        gmail.sender = 'IVS Coordinating Center <ivscc@nviinc.com>'
        gmail.reply_to = '"Thomas, Cynthia C. (GSFC-61A.0)[NVI INC]"<cynthia.c.thomas@nasa.gov>'
        gmail.recipients = ['Mario Berube<mario.berube@nasa.gov>',
                            'Mario Berube<mario.berube@nviinc.com>']
        gmail.priority = True
        ok, msg = gmail.send_message(subject, text, html)
        if ok:
            print('Message {} successfully sent'.format(msg['id']))
        else:
            print('Failed sending messags. [{}]'.format(msg))

        if ivsmail := readDICT(app.ControlFiles.IVSmail):
            for uid in gmail.get_uids('Inbox', '2021/08/07', '17b3'):
                #print(uid)
                msg = gmail.get_msg(uid)

                message = message_from_bytes(msg, ['starred'])
                print(uid, message['Subject'])



import oauth2client
from oauth2client import file, tools


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Create gmail credential file' )
    parser.add_argument('secret')
    parser.add_argument('credential')
    parser.add_argument('scope')

    args = parser.parse_args()

    print(args.secret)
    print(args.credential)

    SCOPES = f'-.{args.scope}'

    print(SCOPES)
    store = file.Storage(args.credential)

    flow = oauth2client.client.flow_from_clientsecrets(args.secret, SCOPES)
    try:
        flags = tools.argparser.parse_args([])
        tools.run_flow(flow, store, flags)
    except Exception as e:
        print('ERROR', str(e))
    #print('2')
    #build('gmail', 'v1', http=creds.authorize(Http()))


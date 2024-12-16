from utils import app


def make_azel(ses_id):

    dbase = app.get_dbase()
    if not (session := dbase.get_session(ses_id)):
        raise Exception(f'{ses_id} does not exist!')
    if not (skd := session.file_path('skd')) and not (skd := session.file_path('skd')):
        raise Exception(f'No schedule for {ses_id}')
    if not (azel := session.file_path('azel')).exists():
        sked, request = app.Applications.Sked['exec'], app.Applications.Sked['azel'].format(path=str(azel))
        ans = app.exec_and_wait(f'{sked} {str(skd)}', request)
        if not azel.exists():
            raise Exception(ans)
    return azel


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser(description='Tools ')
    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('ses_id')

    args = app.init(parser.parse_args())

    try:
        path = make_azel(args.ses_id)
        print(f'{str(path)} created')
    except Exception as exc:
        print(f'azel failed! [{str(exc)}]')

import OpenSSL
import ssl
from datetime import datetime

def check_certificate(url):
    cert=ssl.get_server_certificate((url, 443))
    x509 = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_PEM, cert)
    date = datetime.strptime(x509.get_notAfter().decode('utf-8'), '%Y%m%d%H%M%SZ')
    print(date)

if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser(description='Update web pages')
    parser.add_argument('url', help='url')

    args = parser.parse_args()

    check_certificate(args.url)

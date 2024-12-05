from unidecode import unidecode
import os

def to_ascii(path):
    modified = False
    lines = []
    with open(path, 'rb') as infile:
        for line in infile:  # b'\n'-separated lines (Linux, OSX, Windows)
            line = line.decode('latin-1', 'ignore')
            translated = unidecode(line)
            if line != translated:
                modified = True
            lines.append(translated)

    # Overwrite file if modified
    if modified:
        with open(path, 'w') as outfile:
            for line in lines:
                outfile.write(line)

def test_ascii(path):
    nonascii = bytearray(range(0x80, 0x100))
    with open(path, 'rb') as infile:
        for line in infile:  # b'\n'-separated lines (Linux, OSX, Windows)
            translated = line.translate(None, nonascii)
            if translated != line:
                print(f'Non ASCII character in {line}')
                break

    try:
        open(path, 'r').readlines()
        print(f'{os.path.basename(path)} is ok')
    except Exception as e:
        print(f'Problem with {os.path.basename(path)}. {str(e)}')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Remove non-ascii characters from file' )
    parser.add_argument('-t', '--test', help='test mode', action='store_true')
    parser.add_argument('path')

    args = parser.parse_args()
    if args.test:
        test_ascii(args.path)
    else:
        to_ascii(args.path)


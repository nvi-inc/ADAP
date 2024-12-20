import re

def get_formats(path):

    tag = re.MULTILINE|re.DOTALL
    # Load accepted data code for specific fields
    code, valid_codes = None, {}
    with open(path) as file:
        header, content = file.readline(), file.read()
    if not(version := re.search(r'## (.*)', header)):
        print('Not valid master-format file')
    print(version.group(1))

    tag = re.MULTILINE | re.DOTALL
    for code in re.findall(r'^\s*(\w*) CODES', content, tag):
        pattern = fr'^\s*{code} CODES(.*)^\s*end {code} CODES'
        valid_codes[code] = [l.split()[0] for l in re.findall(pattern, content, tag)[0].splitlines() if l.strip()]
        print(code, valid_codes[code])

def get_ns_codes(path):
    with open(path) as file:
        content = file.read()
    tag = re.MULTILINE | re.DOTALL
    codes = [a[0] for a in re.findall(r'^ (\w{2}) (.{8})', content, tag) if a[1] != '--------']
    print(codes)

def get_media_keys(path):
    tag = re.MULTILINE | re.DOTALL
    with open(path) as file:
        content = re.search(r'type of media(.*)', file.read(), tag).group(1)
    print(content)
    sizes = [a for a in re.findall(r'^\s+([a-zA-Z]) =', content, tag)]
    print(sizes)

get_formats('/Users/mario/MasterSchedules/master-format-v2.0.txt')
get_ns_codes('/Users/mario/MasterSchedules/ns-codes.txt')
get_media_keys('/Users/mario/MasterSchedules/media-key.txt')

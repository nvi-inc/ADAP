import os
import re
import itertools
from pathlib import Path
from collections import defaultdict
from datetime import datetime

from utils import app
from ivsdb import IVSdata


# Read correlator report, Report is stored as text.
class CorrelatorReport:
    def __init__(self, path):
        self.path = Path(path) if isinstance(path, str) else path
        self.ses_id, self.db_name = 'unknown', 'unknown'
        self.is_template, self.text = False, ''
        self.format_version = None

    def __enter__(self):
        self.read()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __eq__(self, other):
        if not self.text:
            self.read()
        if not other.text:
            other.read()

        return self.format_version == other.format_version and self.text == other.text

    def read(self):
        if not self.path.exists():
            self.format_version, self.text = 'missing', 'none'
            return True
        with open(self.path, errors='ignore') as f:
            content = f.read()
            if found := re.search(r'(%CORRELATOR_REPORT_FORMAT \d)', content):
                self.format_version = found.group()
            if text := re.search(r'(\+HEADER(.*)\+END)', content, re.DOTALL):
                self.text = text.group()
                if ses_id := re.search(r'SESSNAME|SESSION +(.*)', self.text):
                    self.ses_id = ses_id.groups()[0]
                if db_name := re.search(r'(DATABASE|VGOSDB)(.*)', self.text):
                    self.db_name = db_name.group().split()[-1].strip()
                self.is_template = bool(re.findall('<comment here>', self.text))
                self.text = '\n'.join([line.lstrip() for line in self.text.splitlines()])
            return True

    def write(self, path):
        if self.format_version != 'missing':
            with open(path, 'w+') as f:
                if self.format_version:
                    print(self.format_version, file=f)
                print(self.text, file=f)

    def save(self, path):
        if not self.text:
            self.read()
        if os.path.exists(path):
            with CorrelatorReport(path) as old:
                old.read()
                if self == old:
                    return False, 'MD5 same'
                # Move old report
                for index in range(1, 10):
                    if (p := path + f'.p{index}') and not os.path.exists(p):
                        old.write(p)
                        break
        self.write(path)
        return True, 'updated'

    def decode_old_format(self, network, names):
        # Make sure the old names are not used
        def clean_old_names(text):
            for old_name, new_name in {'NY ALESUND': 'NYALESUND', 'FORTALEZA': 'FORTLEZA',
                                       'ALGONQUIN': 'ALGOPARK'}.items():
                text = re.sub(old_name, new_name, text, re.IGNORECASE)
            return text

        def decode_line(text):
            if (text := text.strip()) and (words := text.split())[0] in network:
                return words[0], text.split(':', 1)[1].strip()
            return '', text.strip()
        comments = {code: [''] for code in network}
        if info := re.search(r'\+STATION[ _]NOTES(.*)', self.text, re.DOTALL):
            lines = list(itertools.takewhile(lambda x: not x.startswith(('+', '$')), info.groups()[0].splitlines()))
            last = None
            for line in lines:
                code, comment = decode_line(clean_old_names(line))
                if code:
                    last = names[code]
                if last and comment:
                    comments[last].append(comment)

        return comments, {}

    def no_corr_file(self, network):
        return {code: '' for code in network}, {}

    def decode_v3_format(self, network, codes):
        comments, extra = defaultdict(list), defaultdict(list)
        for code in network:
            comments[code].append('')

        if info := re.search(r'\+STATION(.*)\+NOTES(.*)\+CLOCK(.*)', self.text, re.DOTALL):
            for line in info.groups()[0].splitlines():
                if not line.startswith('*') and len(values := line.split()) == 3:
                    if (code := values[0]) in codes and code not in network:
                        network[code] = values[1]
                        comments[code].append('')

            for line in info.groups()[1].splitlines():
                if line and not line.startswith('*'):
                    try:
                        code, comment = line.split(maxsplit=1)
                        if code in network:
                            comments[code].append(comment)
                        elif code == '-' and 'uploaded' not in comment:
                            extra['-'].append(comment)
                        elif sta_list := [sta for sta in code.split('-') if code in network]:
                            extra['|'.join(sta_list)].append(comment)
                    except ValueError:
                        pass

        return comments, extra

    def clean(self, rej_words, rej_exact, paragraph):
        get_missed = re.compile(r'(\d{3}\-\d{4}[ a-zA-Z])(\-\-|through|and) (\d{3}\-\d{4}[ a-zA-Z]*)').findall

        # Check if sentence must be rejected
        def is_rejected(not_interesting_words, text):
            for w in not_interesting_words.split():
                if w not in text:
                    return False
            return True

        def get_cause(text):
            if (index := text.find(' due ')) > -1:
                text = text[index:]
                if (end := re.sub('[,;!?]', '.', text).find('.')) > -1:
                    text = text[:end + 1]
                return text
            return ''

        # Correct sentence with data that have been 'minused'
        def decode_data_minus(text):
            if 'all data' in text:
                return ''
            cause = get_cause(text.lower())
            if not (periods := get_missed(re.sub('[,.;!?]', '', text))):
                return f'Missed few scans{cause}'
            loss = 0
            for period in periods:
                try:
                    start, _, stop = period  # period.split(separator)
                    t1 = datetime.strptime(start.strip()[:8], '%j-%H%M')
                    t2 = datetime.strptime(stop.strip()[:8], '%j-%H%M')
                    loss += (t2 - t1).total_seconds()
                except:
                    pass
            loss /= 3600
            if loss < 0.1:
                return f'Missed few minutes{cause}'
            elif loss < 0.5:
                return f'Missed ~{int(loss * 60):d} minutes{cause}'
            else:
                return f'Missed ~{loss:.1f} hours{cause}'

        phrases = []
        for phrase in paragraph.split('. '):
            if (sentence := phrase.lower()).startswith(('ok', 'no problems')):
                continue
            for word in rej_exact:
                if word in sentence:
                    phrase = ''
                    break
            if phrase:
                for words in rej_words:
                    if is_rejected(words, sentence):
                        phrase = decode_data_minus(sentence) if words == 'data minus' else ''
                        break
                else:
                    if ('scan' in sentence and 'missed' in sentence) or 'no data' in sentence:
                        phrase = decode_data_minus(sentence)

            if phrase := phrase.strip():
                phrases.append(phrase[0].upper() + phrase[1:])

        return '. '.join(phrases) if phrases else ''

    # Read correlator report to extract notes
    def get_notes(self, session, vgosdb, apply_filter=True):
        url, tunnel = app.get_dbase_info()
        with IVSdata(url, tunnel) as dbase:
            name2code = dbase.get_station_name_dict()
        # There are 2 EFLSBERG codes (Ed, Ef) and names dictionary has only one value Ef.
        name2code['EFLSBERG'] = 'Eb'  # Ef is not in any session
        code2name = dict((code.capitalize(), name) for name, code in name2code.items())

        network = {name2code[name]: name for name in vgosdb.station_list}
        network.update({code.capitalize(): code2name[code.capitalize()]  for code in session.included})

        clean_notes = {}
        try:
            if not self.text:
                self.read()
            rejected = app.Applications.APS['CorrNotes']
            rej_words, rej_exact = rejected['words'], rejected['exact']

            if self.format_version == 'missing':  # No corr file use list of station
                notes, extra = self.no_corr_file(network)
            elif self.format_version:  # V3 correlator format
                notes, extra = self.decode_v3_format(network, code2name)
            else:  # Old correlator report
                notes, extra = self.decode_old_format(network, name2code)
            # Add no log comment
            for code, name in network.items():
                if not session.log_path(code).exists():
                    if name in vgosdb.station_list and code not in app.VLBA.stations:
                        notes[code].append('No log')

            notes =dict(sorted([(f"{network[code]}({code})", comments) for code, comments in notes.items()]))
            if extra:
                notes[''] = ''
            for word, comments in extra.items():
                if word == '-':
                    notes['Network'] = comments
                elif '|' in word:
                    keyword = '-'.join([f"{network[code]}({code})" for code in word.split('|')])
                    notes[keyword] = comments
            for code, comments in notes.items():
                paragraph = ' '.join([f'{comment}{"" if comment.endswith(".") else "."}' for comment in comments
                                      if comment.strip()]).strip()
                if apply_filter:
                    paragraph = self.clean(rej_words, rej_exact, paragraph)
                if paragraph or not session.is_intensive:
                    clean_notes[code] = paragraph

            if session.has_vlba:  # Check if vlba and correlator files are available
                cal = session.file_path('vlbacal')
                missing = [] if cal.exists() else [cal.name]
                if not (cor := session.file_path('corr')).exists():
                    missing.append(cor.name)
                if missing:
                    clean_notes["Warning     "] = (f"{' and '.join(missing)} {'is' if len(missing) == 1 else 'are'} "
                                                   f"missing") if missing else None

            return clean_notes
        except Exception as err:
            import traceback
            print(str(err))
            print(traceback.format_exc())
            return clean_notes


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='correlator' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('path')
    args = app.init(parser.parse_args())

    with CorrelatorReport(args.path) as corr:
        for key, cmt in corr.get_notes(apply_filter=False).items():
            print(key, cmt)

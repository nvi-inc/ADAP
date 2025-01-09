from datetime import datetime
import re

from utils import app



class CorrNotes:
    REJWords, REJExact, Names = [], [], []

    clean = re.compile(r'[()/-]').sub
    get_missed = re.compile(r'(\d{3}\-\d{4}[ a-zA-Z])(\-\-|through|and) (\d{3}\-\d{4}[ a-zA-Z]*)').findall

    OLDnames = {'NY ALESUND': 'NYALESUND', 'FORTALEZA': 'FORTLEZA', 'ALGONQUIN': 'ALGOPARK'}

    def __init__(self, session, keep_ok = False):

        self.comments, self.session, self.keep_ok = {}, session, keep_ok
        self.path, _ = session.file_path('corr')

        self.read_notes()

    # Check if sentence must be rejected
    def is_rejected(self, not_interesting_words, sentence):
        for word in not_interesting_words.split():
            if word not in sentence:
                return False
        return True

    # Make sure the old names are not used
    def clean_old_names(self, line):
        for old_name, new_name in self.OLDnames.items():
            pattern = re.compile(old_name, re.IGNORECASE)
            line = pattern.sub(new_name, line)
        return line

    # Get the station name in the line
    def get_station_name(self, line):
        if line.find(':') > -1:
            try:
                line = self.clean_old_names(line.strip())
                index = line.find(':')
                info = CorrNotes.clean(' ', line[:index]).split()
                if len(info) > 1:
                    if (name := f'{info[0]}-VLBA' if info[1] == 'VLBA' else info[0]) in CorrNotes.Names:
                        return name, line[index+1:].strip()
            except:
                pass
        return '', line.strip()

    # Get cause of failure
    def get_cause(self, sentence):
        if 'due' not in sentence.lower():
            return ''
        sentence = sentence[sentence.find(' due '):]
        if (end := re.sub('[,;!?]', '.', sentence).find('.')) > -1:
            sentence = sentence[:end+1]
        return sentence

    # Correct sentence with data that have been 'minused'
    def decode_data_minus(self, sentence):
        if 'all data' in sentence:
            return ''
        cause = self.get_cause(sentence.lower())
        line = re.sub('[,.;!?]', '', sentence)
        if not (periods := CorrNotes.get_missed(line)):
            return f'Missed few scans{cause}'

        loss = 0
        for period in periods:
            try:
                start, _, stop = period #period.split(separator)
                t1 = datetime.strptime(start.strip()[:8], '%j-%H%M')
                t2 = datetime.strptime(stop.strip()[:8], '%j-%H%M')
                loss += (t2 - t1).total_seconds()
            except:
                pass
        loss /= 3600
        if loss < 0.1:
            return f'Missed few minutes{cause}'
        elif loss < 0.5:
            return f'Missed ~{int(loss*60):d} minutes{cause}'
        else:
            return f'Missed ~{loss:.1f} hours{cause}'

    # Decode comment lines and clean specific words
    def decode_comment(self, comments):
        line = re.sub('[;!?]', '.', ' '.join(comments)).strip()
        if 'did not participate' in line.lower():
            return 'Did not participate.'
        if re.search('(no data|not) correlated', line.lower()):
            return line
        if not line.endswith('.'):
            line = line + '.'
        phrases = []
        # Reject sentences with specific word
        for phrase in line.split('. '):
            sentence = phrase.lower()
            if sentence.startswith('ok') and not self.keep_ok:
                continue
            for word in CorrNotes.REJexact:
                if word in sentence:
                    phrase = ''
                    break

            if phrase:
                for words in CorrNotes.REJwords:
                    if self.is_rejected(words, sentence):
                        phrase = self.decode_data_minus(sentence) if words == 'data minus' else ''
                        break
                else:
                    if ('scan' in sentence and 'missed' in sentence) or 'no data' in sentence:
                        phrase = self.decode_data_minus(sentence)

            if phrase := phrase.strip():
                phrases.append(phrase[0].upper() + phrase[1:])

        if phrases:
            return '. '.join(phrases)
        else:
            return ''

    # Read correlator report to extract notes
    def read_notes(self):

        name = None
        # Read correlator report
        with open(self.path, 'r', encoding='utf-8', errors='ignore') as file:
            # Read until station notes found
            found = False
            while line := file.readline():
                line = line.strip().upper()
                if line.startswith('+STATION_NOTES') or 'STATION NOTES:' in line:
                    break
            else:
                return
            # Read until end of note section
            while line := file.readline():
                if not (line := line.strip()):
                    continue
                if line.startswith('+') or line.startswith('$'):
                    break
                sta_name, comment = self.get_station_name(line)
                if sta_name:
                    name = sta_name
                    self.comments[name] = [comment]
                elif name and name in self.comments:
                    self.comments[name].append(comment)

        for name, comment in list(self.comments.items()):
            try:
                comment = self.decode_comment(comment)
            except:
                comment = ' '.join(comment)
            if comment:
                self.comments[name] = comment
            else:
                self.comments.pop(name)

    # Get clean notes
    def get_notes(self):
        return self.comments

    # Load information from control file
    @staticmethod
    def load_static_data(names):
        rejected = app.Applications.APS['CorrNotes']
        CorrNotes.REJWords = rejected['words']
        CorrNotes.REJExact = rejected['exact']
        CorrNotes.Names = names


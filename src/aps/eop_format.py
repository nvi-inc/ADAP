from pandas import read_csv, to_datetime
# from numpy import nan
from ast import literal_eval
from pandas._libs.tslibs.parsing import DateParseError
# from fortranformat import FortranRecordWriter
import datetime as dt
from datetime import timedelta

from difflib import SequenceMatcher


def control_to_dict(control_fname):
    # note that this function does not handle
    # line continuation \ in control files correctly
    # this needs to be implemented
    cont_dict = {}
    section = None
    with open(control_fname, 'r') as rfile:
        for line in rfile:
            line = line.split('*')[0]
            line = line.strip()

            if line == '':
                continue
            if line[0] == '$':
                section = line[1:].lower()
                cont_dict[section] = {}
            elif continue_flag:
                cont_dict[section][var] += line.strip('\\')
            else:

                line_arr = line.strip('\\').split(maxsplit=1)
                if len(line_arr) == 2:
                    var, value = line_arr
                    var = var.lower()
                    value = value.lower()
                    cont_dict[section][var] = value
                else:
                    cont_dict[section][var] = ''

            if line[-1] == '\\':
                continue_flag = True
                line = line[:-1]
            else:
                continue_flag = False

    return cont_dict


class EOP_format:
    def __init__(self, **kwargs):
        self.header = Block('', 'header2.csv', header=True)

        description_fname = kwargs.get('description_fname')
        control_fname = kwargs.get('control_fname')
        self.control_dict = control_to_dict(control_fname)
        self.description = Block('HEADER',
                                 description_fname)

        data_fname = kwargs.get('data_fname')
        data_col_names = kwargs.get('data_col_names')
        self.data = DataBlock(data_fname,
                              data_col_names)
        self.text = ''
        self.make_text()

    def update_dynamic_fields(self):
        header = self.header
        description = self.description
        data = self.data
        now = dt.datetime.now()

        header.update_field('file time', now)
        header.update_field('start time', data.first_date)
        header.update_field('end time', data.last_date)

        description.update_field('generation_time', now)
        description.update_field('data_start', data.first_date)
        description.update_field('data_end', data.last_date)
        description.update_field('[number_of_entries]', data.number_of_entries)
        description.update_subfield('eop_estimated',
                                    data.obs_names,
                                    obs_units=data.obs_units,
                                    control_dict=self.control_dict)

        description.update_field('nutation_type', 'CIO-BASED')

        header.execute_formats()
        description.execute_formats()

    def make_text(self):
        self.update_dynamic_fields()
        text = [ self.header.begin() + self.description.insert()
        text += self.data.insert()
        text += self.header.end()
        self.text = ''.join(text)


class SubField:
    def __init__(self, sup_field, query_text):
        self.name = sup_field
        self.cls = self.__class__.__name__
        self.field_list = []  # list of type field
        self.query_text = query_text

    def text(self, max_len):
        text = ''
        max_sub_len = 2 + max([len(field.name) for field in self.field_list])
        for field in self.field_list:
            msg = field.message
            if msg is None:
                msg = 'NONE'
            text += f'{self.name:<{max_len}} {field.name:<{max_sub_len}} {msg}\n'
        return text

    def get_constraints(self, obs_units, control_dict):
        # this is specific to updating the eop_estimated subfields
        constraints = control_dict['constraints']
        if constraints['nutation'] != 'no':
            raise (NotImplementedError('Nutation constraint is not being parsed!'))

        query = constraints['earth_orientation'].strip()
        const_list = query.split()[2:]  # constraints
        del const_list[3:5]
        const_names = ['xPol', 'yPol', 'dUT1', 'xPolR', 'yPolR', 'LOD']
        const_dict = {}
        # print(self.field_list)
        # print(f'{const_list=}')
        # print(f'{const_names=}')
        for const, name in zip(const_list, const_names):
            if obs_units[name] == '[s]':
                # somewhat funny way to convert units
                # the control file lists UT1 params in ms
                # eop time series v3.1 needs seconds
                const = float(const) / 1e3

            const_dict[name] = f'{const} {obs_units[name][1:-1]}'

        for field in self.field_list:
            field.message = const_dict.get(field.name, None)
            if 'R' in field.name:
                # correct for rates. the format specifies derivatives
                # explicitly rather than leaving as (R)ate
                name = field.name[:-1] + '_DER_1'
                field.name = name
        return self.field_list

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return f'SubField({self.name},{self.field_list})'


class DataBlock:
    def __init__(self, data_fname, col_names_fname):
        self.data = ''
        self.first_date = None
        self.last_date = None
        self.obs_names = []
        self.obs_units = {}
        with open(data_fname, 'r') as rfile:
            counter = 0
            for line in rfile:
                if line[0] == '#' or line == '\n':
                    continue
                if self.first_date is None:
                    self.first_date = line.split()[0]
                counter += 1
                if counter == 10 and True:
                    break
                line_write = line.replace("-0 ", "NA ")
                self.data += line_write
            self.last_line = line_write.split()
            self.last_date = self.last_line[0]
            self.number_of_entries = counter

        print(self.data)
        with open(col_names_fname, 'r') as rfile:
            lines = rfile.readlines()
            self.col_names = lines[0].split()
            self.col_units = lines[1].split()

        obs_ind = list(range(1, 6)) + list(range(19, 24))  # indices of observables
        print(self.last_line)
        for i in obs_ind:
            print(self.last_line[i])
            if self.last_line[i] != 'NA':
                self.obs_names.append(self.col_names[i])
                self.obs_units[self.col_names[i]] = self.col_units[i]
                print('OBS', self.col_names[i], self.obs_names[-1], self.obs_units[self.col_names[i]])

        #        self.obs_names = [self.col_names[i] for i in obs_ind]
        #        self.obs_units = [self.col_units[i] for i in obs_ind]

        self.first_date = self.julian_to_dt(self.first_date)
        self.last_date = self.julian_to_dt(self.last_date)

    def insert(self):
        text = '+DATA\n'
        text += self.data
        text += '-DATA\n'
        return text

    def julian_to_dt(self, jtime):
        # converts jtime float into normal datetime format
        # accuracy double checked but if in doubt recheck!
        jtime = float(jtime)
        jdate, jfraction = divmod(jtime, 1)

        # magic numbers refer to conversion of the modified julian date
        date = dt.datetime.fromordinal(int(jdate - 1721424.5 + 2400000.5));
        date += timedelta(hours=int(jfraction * 24))
        return date


class Block:
    def __init__(self, block_name, csv_fname, header=False):
        df = read_csv(csv_fname)
        self.df = df
        self.fields = [Field(name, message=message) for name, message in zip(df.name, df.message)]
        self.csv_fname = csv_fname
        self._field_dict = {name: field for name, field in zip(df.name, self.fields)}
        self.header = header
        self.block_name = block_name

        # handle subfields
        is_subfield = (self.df.fmt == 'array')
        subfield_idx = df[is_subfield].index.values

        for i in subfield_idx:
            self.fields[i] = SubField(self.fields[i].name, self.fields[i].message)

    def update_field(self, field_name, msg):

        for field in self.fields:
            if field.name.lower() == field_name.lower():
                field.message = msg

    def update_subfield(self, name, sfield_names, **kwargs):

        for field in self.fields:
            if field.name.lower() == name.lower():
                if field.cls == 'Field':
                    raise (Exception('Trying to update a Field with subfield command'))
                subfield_list = [Field(name) for name in sfield_names]
                field.field_list = subfield_list

                # the following function is specific to eop_estimated
                # i'm abusing the fact that there's only one subfield
                subfield_list = field.get_constraints(kwargs['obs_units'],
                                                      kwargs['control_dict'])

    @property
    def block_text(self):
        self._block_text = ''
        max_len = max([len(field.name) for field in self.fields])
        for field in self.fields:
            if field.cls == 'Field':
                self._block_text += f'{field.name:<{max_len}} {field.message}\n'
            elif field.cls == 'SubField':
                self._block_text += field.text(max_len)

        return self._block_text

    def begin(self):

        if self.header:
            text = ''
            add_space = False
            for field in self.fields:
                text += field.message
                if field.name == 'Document Type' or add_space:
                    text += ' '
                    add_space = True
            return text + '\n'
        else:

            return f'+{self.block_name}\n'

    def end(self):
        if self.header:
            text = ''
            for field in self.fields:
                # second character isn't in the footer
                if field.name == 'SecondCharacter':
                    continue
                # footer ends with the format version
                if field.name == 'Format Version':
                    text += ' '
                    text += field.message
                    break

                text += field.message
            text += ' END'
            return text
        else:
            return f'-{self.block_name}\n'

    def insert(self):
        text = ''
        text += self.begin()
        text += self.block_text
        text += self.end()
        return text

    def execute_formats(self):
        raw_formats = self.df.fmt
        formatted_message = ''
        for field, fmt in zip(self.fields, raw_formats):

            try:
                fmt = literal_eval(fmt)
            except:
                pass

            if type(fmt) is list:

                if len(fmt) == 1:
                    formatted_message = fmt[0]

                if field.message not in fmt:
                    techniques = field.message.split('+')
                    for technique in techniques:
                        if technique not in fmt:
                            print('raised format exception')
                            print((f'"{technique}" not allowed for {field.name}. Allowed values are {fmt}\n'))

                formatted_message = field.message
            elif fmt == 'datetime':
                # execute the datetime format
                raw_dt = field.message
                try:
                    datetime = to_datetime(raw_dt)
                    formatted_message = datetime.strftime('%Y-%m-%dT%X')

                except(DateParseError, ValueError):
                    raise (Exception(f'Date parsing for {field.name} failed. Cannot parse "{raw_dt}"'))


            elif fmt == 'array':
                # deal with the array format
                # this is a special case for EOP_ESTIMATED
                pass

            elif type(fmt) == float:
                # it only resolves to float when the field is empty
                # and therefore query = nan

                # nothing to do
                formatted_message = field.message


            else:
                # this should only occur for fortran format types
                # execute fortran type
                # f_format = FortranRecordWriter(fmt)
                # formatted_message= f_format.write([field.message])
                formatted_message = field.message

            field.message = formatted_message


class Field:
    def __init__(self, name, message=None):
        self.name = name
        self.message = message
        self.cls = self.__class__.__name__

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return f'Field({self.name},{self.message})'


eop_24 = EOP_format(description_fname='description_24_new.csv',
                    control_fname="2023a_24h_eop.cnt",
                    data_fname='2023a_24h_last.eops',
                    data_col_names='data_col_names',
                    )

print(eop_24.text)

# eop_int = EOP_format(description_fname = 'description_int.csv',
#                  description_cnt = "2023a_int_standalone.cnt" ,
#                  data_fname = '2023a_int_last.eopm',
#                  data_col_names = 'data_col_names',
#                  )


# desc = Block('session','HEADER','description.csv',control_file="2023a_int_standalone.cnt")
# header =Block('','header2.csv',header=True)
# data_block = DataBlock('session','test_eob.eob')


def to_float(value):
    if isinstance(value, str):
        value = value.strip()
    if not value:
        return 0
    if isinstance(value, (int, float)):
        return value
    return float(value)

def print_record(rec, reasons):
    date = rec['Date'].strftime('%Y-%m-%d')
    in_sched = data['Scans'] > 0
    percent = max(0, to_float(rec['Total Loss']))
    stat = sorted([(to_float(rec[reason]), reason) for reason in reasons], reverse=True)
    why = stat[0][1] if percent > 0 else ''
    print(f'{rec["Session"]},{rec["Station ID"]},{date},{in_sched}', end='')
    print(f',{percent:0.1f},{why},{rec["Problem"]}')


if __name__ == '__main__':
    from utils import app, security
    import openpyxl as xl

    opt, param = app.init(db='mysql')

    database = security.get(opt['db'])

    records = []
    for year in range(2017, 2021):
        path = app.config['nc report'].format(year=year)
        xlsx = xl.load_workbook(path, read_only=True, data_only=True)
        # Get Details sheet
        sheet = xlsx['Details']
        # Read header
        header = []
        rows = sheet.iter_rows()
        row = next(rows)
        for cell in row:
            if not cell.value:
                break
            header.append(cell.value.strip())
        nbr_columns = len(header)
        reasons = header[-13:-1]
        # Read data
        for row in rows:
            data = {}
            for cell in row:
                if isinstance(cell, xl.cell.read_only.ReadOnlyCell) and cell.column <= nbr_columns:
                    # if cell.column > nbr_columns:
                    # print('{} {} : {}'.format(cell.row, cell.column, cell.value))
                    data[header[cell.column - 1]] = cell.value
            if len(data) != nbr_columns:
                continue
            if data.get('Session', None) and data['Session'].startswith(('R1', 'R4')):
                records.append(data)

    # Print header and records
    print('Session,Station,Date,Scheduled,Loss,Reason,Comments')
    for data in sorted(records, key=lambda d: (d['Date'], d['Station ID'])):
        print_record(data, reasons)


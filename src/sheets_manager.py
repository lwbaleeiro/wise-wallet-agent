import gspread

from pathlib import Path
from gspread_dataframe import get_as_dataframe, set_with_dataframe

BASE_DIR = Path(__file__).parent.resolve()
SHEETS_CREDENTIALS_PATH = BASE_DIR.parent / "config" / "sheets-credentials.json"

def get_sheets_service():
    try:
        gc = gspread.service_account(filename=SHEETS_CREDENTIALS_PATH)
        return gc
    except Exception as e:
        raise ValueError(f'Erro ao inicializar Google Sheets: {str(e)}')

def update_sheet(gc, df, spreadsheet_name, worksheet_name):
    try:
        sh = gc.open(spreadsheet_name)
    except gspread.SpreadsheetNotFound:
        sh = gc.create(spreadsheet_name)

    try:
        worksheet = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        worksheet = sh.add_worksheet(worksheet_name, rows=100, cols=20)

    # Formatação inicial
    if worksheet.row_count == 0:
        headers = df.columns.tolist()
        worksheet.append_row(headers)

        header_format = {
            'backgroundColor': {'red': 0.2, 'green': 0.4, 'blue': 0.6},
            'textFormat': {'bold': True}
        }
        worksheet.format('A1:Z1', header_format)

    # Adiciona dados
    existing = worksheet.get_all_records()
    new_data = df[~df.apply(tuple, 1).isin([tuple(x.values()) for x in existing])]

    if not new_data.empty:
        set_with_dataframe(worksheet, new_data, row=len(existing) + 2, include_column_header=False)

    negative_rows = [
        i+2 for i, row in enumerate(existing + new_data.to_dict('records')) 
        if row['valor'] < 0
    ]

    if negative_rows:
        worksheet.format(
            f'C{",C".join(map(str, negative_rows))}',
            {"backgroundColor": {"red": 1, "green": 0.9, "blue": 0.9}}
        )

    return True

    
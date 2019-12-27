from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict

from openpyxl.styles import Border, Side, Font, Alignment

from hatsudenki.packages.command.excel.excel_data import ExcelData, CellStyle
from hatsudenki.packages.command.master.loader import MasterTableLoader


class MasterAssignerExcel(ExcelData):

    def __init__(self, file_path: Path, header: List[str]):
        super().__init__(file_path)
        self.header = header
        self.rows = []

    def add_row(self, row: Dict[str, str]):
        self.rows.append(row)

    def render(self, sheet_name: str):
        if sheet_name not in self.book.sheetnames:
            self.book.create_sheet(sheet_name)
        self.write_row(sheet_name, 0, 0, self.header, style=CellStyle.GREEN)

        for idx, r in enumerate(self.rows):
            c = []
            for h in self.header:
                c.append(r.get(h, ''))
            self.write_row(sheet_name, idx + 1, 0, c)


@dataclass
class MasterDataAssignerExcelRenderer:
    master_loader: MasterTableLoader

    def render_excel(self, file_path: Path):
        excel = MasterAssignerExcel(file_path, ['エクセル名', 'シート名', 'テーブル名', 'クライアント', 'サーバー', '企画'])
        excel.create()

        for p, m in self.master_loader.iter():
            excel.add_row({
                'エクセル名': m.excel_name,
                'シート名': m.excel_sheet_name,
                'テーブル名': m.table_name,
                'クライアント': ', '.join(m.assignee_clients),
                'サーバー': ', '.join(m.assignee_servers),
                '企画': ', '.join(m.assignee_planners)
            })
        excel.render('担当者一覧')
        border = Border(
            top=Side(style='thin', color='808080'),
            bottom=Side(style='thin', color='808080'),
            left=Side(style='thin', color='808080'),
            right=Side(style='thin', color='808080')
        )
        font = Font(
            name='MS Gothic',
            size=11
        )
        bold_font = Font(
            name='MS Gothic',
            size=11,
            bold=True
        )

        w = {}

        sheet = excel.get_sheet('担当者一覧')

        for idx, row in enumerate(excel.iter_row('担当者一覧')):
            for c in row:
                c.border = border
                if idx is 0:
                    c.font = bold_font
                    c.alignment = Alignment(horizontal='center')
                else:
                    c.font = font
                v = c.value if c.value else ''

                wi = self._major_width(str(v))
                o = w.get(c.column, 0)
                w2 = max(o, wi)
                if w2 > o:
                    sheet.column_dimensions[c.column].width = w2 + 0.8
                    w[c.column] = w2

        excel.save_file()

    def _major_width(self, text: str):
        from unicodedata import east_asian_width

        width_dict = {
            'F': 2,  # Fullwidth
            'H': 1,  # Halfwidth
            'W': 2,  # Wide
            'Na': 1,  # Narrow
            'A': 2,  # Ambiguous
            'N': 1  # Neutral
        }

        chars = [char for char in text]
        east_asian_width_list = [east_asian_width(char) for char in chars]
        width_list = [width_dict[east_asian_width] for east_asian_width in east_asian_width_list]
        return sum(width_list)

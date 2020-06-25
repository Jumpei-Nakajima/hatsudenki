from pathlib import Path
from typing import Dict

from openpyxl.styles import Font, Color
from openpyxl.utils import coordinate_to_tuple

from hatsudenki.packages.command.excel.excel_data import ExcelData, CellStyle
from hatsudenki.packages.command.master.table import MasterTable
from hatsudenki.packages.command.stdout.output import ToolOutput


class MasterExcelBook(ExcelData):
    TABLE_NAME_SHEET_NAME = 'テーブル名'
    RELATION_SHEET_NAME = 'リレーション'

    def __init__(self, file_path: Path, book_info: Dict[str, MasterTable]):
        super().__init__(file_path)
        self.sort_dict = {}
        self.table_infos = book_info
        if file_path.exists():
            self.load()
        else:
            self.create()

        s = self.book.named_styles
        if CellStyle.RED.name not in s:
            self.book.add_named_style(CellStyle.RED)
        if CellStyle.BLUE.name not in s:
            self.book.add_named_style(CellStyle.BLUE)
        if CellStyle.GREEN.name not in s:
            self.book.add_named_style(CellStyle.GREEN)

        self._generate_table_sheets()
        self.book._sheets.sort(
            key=lambda t: self.sort_dict[t.title] if t.title in self.sort_dict else 0,
            reverse=True
        )

    def _generate_table_sheets(self):
        """
        マスターテーブルに対応するシートを生成
        """
        origin = 0
        self.sort_dict = {}
        # 所属するテーブルでループ
        for idx, (sheet_name, table) in enumerate(self.table_infos.items()):
            ToolOutput.anchor(f'[{sheet_name}]シート作成開始...')

            if sheet_name in self.book.sheetnames:
                # すでに対応するシートが存在する
                sheet = self.book[sheet_name]
                old_header = self.get_header_values(sheet_name)
                self.sort_dict[sheet_name] = table.data['priority'] if 'priority' in table.data else 0
            else:
                sheet = self.book.create_sheet(sheet_name, origin + idx)
                ToolOutput.out(f'[{sheet_name}]シート を追加します')
                old_header = {}
                self.sort_dict[sheet_name] = table.data['priority'] if 'priority' in table.data else 0
            tail_coord = old_header.get('TAG', 'A1')

            # カラム列の生成
            for idx_2, (k, c) in enumerate(table.columns.items()):
                # すでに列が存在するか？
                coord = old_header.get(c.excel_raw_header_name, None)
                if coord is None:
                    # 新規に追加された列である
                    ToolOutput.out(f'{c.excel_raw_header_name} 列が[{tail_coord}]に追加されました')

                    row_idx, col_idx = coordinate_to_tuple(tail_coord)
                    sheet.insert_cols(col_idx, 1)
                    cell = sheet[tail_coord]
                    tail_coord = cell.offset(row=0, column=1).coordinate
                else:
                    cell = sheet[coord]

                cell.style = CellStyle.RED.name
                # 特殊なカラムの処理
                if c.is_no_pack:
                    # packに出力しない列
                    cell.style = CellStyle.GREEN.name
                if c.is_relation:
                    # 参照が設定されている列
                    cell.value = c.excel_raw_header_name
                    cell.hyperlink = c.get_link_label()
                    cell.font = Font(name="Meiryo UI", size=10, underline="single",
                                     color=Color(rgb=None, indexed=None, auto=None, theme=10, tint=0.0, type="theme"))
                else:
                    # 普通の列
                    cell.value = c.excel_header_name
            ToolOutput.pop('OK')

            cell = sheet[tail_coord]
            cell.style = CellStyle.BLUE.name
            cell.value = 'TAG'

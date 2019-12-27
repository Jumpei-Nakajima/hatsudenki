from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple

from openpyxl.worksheet.worksheet import Worksheet

from hatsudenki.packages.command.excel.excel_data import ExcelData
from hatsudenki.packages.command.master.column import MasterColumn, ColumnRelation, ColumnChose
from hatsudenki.packages.command.master.table import MasterTable
from hatsudenki.packages.command.master.tag.loader import MasterTag
from hatsudenki.packages.command.stdout.output import ToolOutput
from hatsudenki.packages.command.util.base_info import BaseInfo


class MasterExcelData(ExcelData):
    """
    企画入力マスターデータExcel
    """

    def __init__(self, file_path: Path):
        super().__init__(file_path)
        self.load()

    def iter_table_sheet(self):
        return (sheet for sheet in self.book.worksheets)

    def iter_formula_table_sheet(self):
        return (sheet for sheet in self.formula_book.worksheets)


class MasterExcelSheet(object):
    """
    企画入力マスターシート
    """

    def __init__(self, book: 'MasterExcel', sheet: Worksheet, formula_sheet: Worksheet, table: MasterTable, loader):
        from hatsudenki.packages.command.master.exporter.loader import MasterExcelLoader
        self.sheet = sheet
        self.formula_sheet = formula_sheet
        self.table = table
        self.loader: MasterExcelLoader = loader
        ToolOutput.out(f'[MasterExcel]Sheet準備 {self.sheet.title} = {table.table_name}')
        self.book = book

        self.resolved_headers = self._resolve_headers()
        self.datas: Dict[str, Dict[str, Tuple[MasterColumn, any]]] = self._resolve()

    @property
    def reference_label(self):
        return self.table.reference_column

    @property
    def cursor_labels(self):
        return self.table.cursor_labels

    @property
    def has_range(self):
        return self.table.range_key is not None

    @property
    def except_label(self):
        return f'{self.book.data.name} {self.sheet.title} "{self.table.full_path}:0"'

    def _resolve_headers(self):
        h = next(self.formula_sheet.rows)
        ret = {}
        for cell in h:
            c = next((c for c in self.table.columns.values() if cell.value == c.excel_header_name), None)
            ret[cell.value] = c

        return ret

    def _check_relation(self, table: MasterTable, val, header: str = None):
        excel = self.loader.get_by_excel_name(table.excel_name + '.xlsx')
        s = excel.get_sheet_by_name(table.excel_sheet_name)
        d = s.find_row(val)
        if d is None:
            target_info = f'{table.label} "{table.rel_path}:0"'
            raise Exception(f'参照解決に失敗 {self.except_label} {header} {val}\n{target_info}')

        return val

    def _check_relation_by_column(self, column: ColumnRelation, val):
        t = column.get_target_table()
        return self._check_relation(t, val, column.excel_raw_header_name)

    def _resolve_chose(self, column: ColumnChose, sel, val):
        """
        choseカラムの解決

        :param column: カラムインスタンス
        :param sel: セレクタの値
        :param val: ID
        :return:
        """
        v = column.resolve_value(sel, val, is_raw=False)
        if isinstance(v, MasterTable):
            # masterの場合はリレーション先が存在するかチェックする
            # enumは解決時にチェックされているので不要
            self._check_relation(v, val, column.excel_raw_header_name)
        # strで固定しないとMessagePackに変換できない
        return str(v)

    def _find_header_index(self, key_str: str):
        return next((idx for idx, key in enumerate(self.resolved_headers.keys()) if key == key_str), None)

    def _check_tag_level(self, tag_level: MasterTag):

        if tag_level.is_debug and not self.is_enable_debug:
            # デバッグタグはデバッグモードが有効でないときは処理しない
            return False

        if self.out_tag_level.is_only:
            # Onlyが設定されている場合は完全一致のみ
            if tag_level.name == self.out_tag_level.name:
                return True
            # と見せかけてincludeに入ってるやつもOK
            if tag_level.name in self.out_tag_level.include:
                return True
            return False

        if tag_level.is_only:
            # Onlyは指定タグに設定されたときしか出力されない
            return False

        # TAGレベル判定
        if tag_level.level > self.out_tag_level.level:
            return False
        return True

    @property
    def out_tag_level(self):
        return self.loader.out_tag

    @property
    def is_enable_debug(self):
        return self.loader.is_debug

    @staticmethod
    def _conv_datetime(value: datetime):
        # msが存在しない場合はそのまま返して良い
        if value.microsecond == 0:
            return value
        return datetime.fromtimestamp(round(value.timestamp(), -1))

    def _resolve(self):
        rows = self.sheet.rows
        next(rows)
        tag_idx = self._find_header_index('TAG')

        ref_idxes = [self._find_header_index(cursor_column.excel_header_name) for cursor_column in self.cursor_labels]

        datas = {}
        for row in rows:
            ret = {}

            # TAGレベル判定
            tag_level = str(row[tag_idx].value)
            tag_info = self.loader.resolve_tag(tag_level)

            if row[ref_idxes[0]].value is None or not self._check_tag_level(tag_info):
                continue

            if tag_info is None:
                raise Exception(f'無効なタグ指定 {tag_level}')

            # 既存データの検索
            hash = row[ref_idxes[0]].value
            already = datas.get(hash)
            if already and self.has_range:
                range = row[ref_idxes[1]].value
                already = already.get(range)

            # TAGレベルが現在データよりも低い場合は上書きしない
            if already:
                at = self.loader.resolve_tag(str(already['__TAG__'][1]))
                if at.level > tag_info.level:
                    # 前データのほうが強いのでスキップ
                    continue
                elif at.level == tag_info.level:
                    if at.is_debug == tag_info.is_debug:
                        # デバッグ属性が同一かつレベルが同じ＝Excel内でキーが重複している
                        if self.has_range:
                            raise Exception(
                                f'キーが重複しています {self.book.data.name} {self.sheet.title} {self.table.hash_key.excel_raw_header_name} {hash} - {self.table.range_key.excel_raw_header_name} {range} "{self.table.full_path}:0"')
                        else:
                            raise Exception(
                                f'キーが重複しています {self.book.data.name} {self.sheet.title} {self.table.hash_key.excel_raw_header_name} {hash}"{self.table.full_path}:0"')

                    if not tag_info.is_debug:
                        # すでにデバッグデータで上書きされているので処理しない
                        continue

            for header_key, col in zip(self.resolved_headers.keys(), row):
                if header_key == 'TAG':
                    ret['__TAG__'] = None, col.value
                    continue

                header = self.resolved_headers[header_key]
                if header is None:
                    continue

                # エクセルからロードした値のチェック
                value = col.value
                # 値がdatetime型の時
                if type(value) is datetime:
                    # 値のコンバートが必要な場合がある
                    value = self._conv_datetime(value)
                ret[header.python_name] = header, value

            if self.has_range:
                if hash not in datas:
                    datas[hash] = {ret[self.table.range_key.python_name][1]: ret}
                else:
                    datas[hash][ret[self.table.range_key.python_name][1]] = ret
            else:
                datas[hash] = ret

        return datas

    def iter(self):
        """
        1行のデータのイテレータ

        :return: dict
        """

        def _(k, c, v, ret: dict, hm: dict):
            if c is None:
                return
            if isinstance(c, ColumnRelation):
                ret[k] = self._check_relation_by_column(c, v)
            elif isinstance(c, ColumnChose):
                ret[k] = self._resolve_chose(c, hm[c.selector][1], v)
            else:
                ret[k] = c.generate_value(v)

        for hash_map in self.datas.values():
            if self.has_range:
                for mapped_data in hash_map.values():
                    ret = {}
                    for k, (c, v) in mapped_data.items():
                        _(k, c, v, ret, mapped_data)
                    yield ret
            else:
                ret = {}
                for k, (c, v) in hash_map.items():
                    _(k, c, v, ret, hash_map)
                yield ret

    def find_row(self, value):
        return self.datas.get(value)


class MasterExcel(BaseInfo):
    def __init__(self, base_path: Path, full_path: Path, table_data: Dict[str, MasterTable], loader, *args,
                 **kwargs):
        from hatsudenki.packages.command.master.exporter.loader import MasterExcelLoader
        super().__init__(base_path, full_path, *args, **kwargs)
        self.data = MasterExcelData(Path(full_path))
        self.table = table_data
        self.loader: MasterExcelLoader = loader
        self.table_sheets: Dict[str, MasterExcelSheet] = {}
        ToolOutput.anchor(f'{self.data.name} をよみこみます')
        for sheet in self.data.iter_table_sheet():
            if sheet.title not in self.table:
                continue
            self.table_sheets[sheet.title] = self._create_sheet(sheet)
        ToolOutput.pop('OK')

    def iter_table_sheet(self):
        return (sheet for sheet in self.table_sheets.values())

    def get_sheet_by_name(self, sheet_name: str):
        return self.table_sheets[sheet_name]

    def _create_sheet(self, sheet):
        return MasterExcelSheet(self, sheet, self.data.formula_book[sheet.title],
                                self.table[sheet.title], self.loader)

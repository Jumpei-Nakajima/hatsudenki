from collections import defaultdict
from pathlib import Path
from typing import Dict

from hatsudenki.packages.command.loader.base import LoaderBase
from hatsudenki.packages.command.master.enum.loader import EnumLoader
from hatsudenki.packages.command.master.table import MasterTable
from hatsudenki.packages.command.stdout.output import ToolOutput


class MasterTableLoader(LoaderBase[MasterTable]):
    """
    Master定義YAMLのローダー
    """

    def __init__(self, base_path: Path, enum_loader: EnumLoader):
        super().__init__(base_path)
        self.ext = '.yml'
        self.ref_table_name: Dict[str, MasterTable] = {}
        self.ref_excel_name: Dict[str, Dict[str, MasterTable]] = {}
        self.enum_loader = enum_loader

    def _load(self, path: Path):
        return MasterTable(self.base_path, path, self)

    def setup(self, dir_name=''):
        super().setup(dir_name)

        ToolOutput.anchor('MasterExcelを準備します')
        d = defaultdict(dict)
        for k, v in self.iter():
            self.ref_table_name[v.table_name] = v
            d[v.excel_name][v.excel_sheet_name] = v
        self.ref_excel_name = dict(d)
        ToolOutput.pop()

    def get_by_table_name(self, table_name: str):
        if not table_name.startswith('master_'):
            table_name = 'master_' + table_name
        return self.ref_table_name.get(table_name, None)

    def get_by_excel_name(self, excel_name: str):
        return self.ref_excel_name.get(excel_name, None)

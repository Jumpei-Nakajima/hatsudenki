from pathlib import Path
from typing import List, Dict

from hatsudenki.packages.command.files import yaml_load
from hatsudenki.packages.command.master.column import ColumnChose
from hatsudenki.packages.command.master.table import MasterTable
from hatsudenki.packages.command.stdout.output import ToolOutput
from hatsudenki.packages.command.util.base_info import BaseInfo


class MasterDataYaml(BaseInfo):
    def __init__(self, base_path: Path, full_path: Path, table: MasterTable, loader, *args, **kwargs):
        from hatsudenki.packages.command.master.importer.loader import MasterDataYamlLoader
        super().__init__(base_path, full_path, *args, **kwargs)
        self.data: List[Dict[str, any]] = yaml_load(full_path)
        self.table = table
        self.loader: MasterDataYamlLoader = loader

    @property
    def table_name(self):
        return self.table.table_name

    def iter(self):
        ToolOutput.anchor()
        for data in self.data:
            ret = {}
            for key, col in data.items():
                ToolOutput.anchor()
                col_scheme = self.table.columns.get(key)
                if col_scheme is None:
                    ToolOutput.print(f'{key} が見つかりません "{self.full_path}:0"\n"{self.table.full_path}:0"')
                    ToolOutput.pop()
                    continue

                if isinstance(col_scheme, ColumnChose):
                    # Choseはselectorの値が無いとExcel上の値が確定しない
                    ret[col_scheme.excel_header_name] = col_scheme.resolve_value(data[col_scheme.selector], col,
                                                                                 is_raw=True, ret_ref_name=True)
                else:
                    ret[col_scheme.excel_header_name] = col_scheme.reverse_value(col)
                ToolOutput.pop()
            yield ret
        ToolOutput.pop()

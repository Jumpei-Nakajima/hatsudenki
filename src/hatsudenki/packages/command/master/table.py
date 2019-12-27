from pathlib import Path
from typing import Dict

from hatsudenki.packages.command.files import yaml_load
from hatsudenki.packages.command.master.column import MasterColumn, ColumnFactory
from hatsudenki.packages.command.stdout.output import snake_to_camel
from hatsudenki.packages.command.util.base_info import BaseInfo


class MasterTable(BaseInfo):
    """
    Masterデータのスキーマ定義
    """

    def __init__(self, base_path: Path, full_path: Path, table_loader, *args, **kwargs):
        from hatsudenki.packages.command.master.loader import MasterTableLoader
        super().__init__(base_path, full_path, *args, **kwargs)
        self.data = yaml_load(full_path)
        self.table_loader: MasterTableLoader = table_loader
        self.columns: Dict[str, MasterColumn] = {}
        self.shadow_columns: Dict[str, MasterColumn] = {}
        self._build_columns()
        self.hash_key = next((c for c in self.columns.values() if c.is_hash_key), None)
        if self.hash_key is None:
            raise Exception(f'{self.table_name} に hashキーが設定されていません "{full_path}:1"')
        self.range_key = next((c for c in self.columns.values() if c.is_range_key), None)

        self.serial_key = next((c for c in self.columns.values() if c.is_serial_key), None)

    def _build_columns(self):
        self.columns = {k: ColumnFactory.create(self.table_name, k, c, self.table_loader) for k, c in
                        self.data.get('column', {}).items()}

        self.shadow_columns = {k: ColumnFactory.create(self.table_name, k, c, self.table_loader) for k, c in
                               self.data.get('shadow_column', {}).items()}

    @property
    def table_name(self):
        return 'master_' + self.normalize_name

    @property
    def class_name(self):
        return 'Master' + snake_to_camel(self.normalize_name)

    @property
    def excel_sheet_name(self):
        return self.data.get('name', self.table_name)

    @property
    def excel_name(self):
        return self.data['excel']

    @property
    def label(self):
        return f'{self.rel_path}【{self.excel_name}】{self.excel_sheet_name}'

    @property
    def reference_column(self):
        r = next((c for c in self.columns.values() if c.column_name == 'name'), None)
        if r is None:
            return self.columns[next(self.columns.keys().__iter__())]
        return r

    @property
    def cursor_labels(self):
        ret = [self.hash_key]
        if self.range_key is not None:
            ret.append(self.range_key)

        return ret

    @property
    def assignee(self):
        return self.data.setdefault('assignee', {})

    @property
    def assignee_clients(self):
        return self.assignee.get('clients', [])

    @assignee_clients.setter
    def assignee_clients(self, val):
        self.assignee['clients'] = val

    @property
    def assignee_servers(self):
        return self.assignee.get('servers', [])

    @assignee_servers.setter
    def assignee_servers(self, val):
        self.assignee['servers'] = val

    @property
    def assignee_planners(self):
        return self.assignee.get('planners', [])

    @assignee_planners.setter
    def assignee_planners(self, val):
        self.assignee['planners'] = val

    @property
    def is_out_pack(self):
        return self.data.get('out_pack', False)

    @property
    def description(self):
        return self.data.get('description', None)

    def find_column_by_excel_name(self, excel_name: str):
        for k, c in self.columns.items():
            if c.excel_raw_header_name == excel_name:
                return c

from __future__ import annotations

from pathlib import Path
from typing import Tuple

import yaml

from hatsudenki.packages.command.master.compare.schema import Schema


class YMLSchemaReader(object):
    def __init__(self, base_path: Path):
        self.base_path = base_path
        self._data = []

    def setup(self) -> YMLSchemaReader:
        print(f'loading...yml files...')
        yml_files = [open(p) for p in self.base_path.glob('**/*.yml')]
        self._data = [yaml.safe_load(f) for f in yml_files]
        [f.close() for f in yml_files]
        return self

    def schema(self) -> Schema:
        # 初期の空スキーマを作成する
        groups = {f['excel'] for f in self._data}
        schema = Schema([Schema.Group(g, []) for g in groups])
        # 割当
        for k, tbl in (self._create_table(d) for d in self._data):
            next(g for g in schema.groups if g.name == k).tables.append(tbl)
        return schema

    def _create_table(self, dic: dict) -> Tuple[str, Schema.Group.Table]:
        print(f'    loading... {dic["name"]}')
        columns = [self._create_column(dic['column'][d]) for d in dic['column']]
        return dic['excel'], Schema.Group.Table(dic['name'], columns)

    @staticmethod
    def _create_column(dic: dict) -> Schema.Group.Table.Column:
        return Schema.Group.Table.Column(dic.get('name', dic.get('to')))

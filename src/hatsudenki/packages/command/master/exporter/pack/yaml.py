from pathlib import Path
from typing import List, Dict

from hatsudenki.packages.command.files import yaml_load
from hatsudenki.packages.command.master.table import MasterTable
from hatsudenki.packages.command.util.base_info import BaseInfo


class MasterExportYaml(BaseInfo):
    def __init__(self, base_path: Path, full_path: Path, table: MasterTable, loader, *args, **kwargs):
        from hatsudenki.packages.command.master.exporter.pack.loader import MasterExportYamlLoader
        super().__init__(base_path, full_path, *args, **kwargs)
        self.data: List[Dict[str, any]] = yaml_load(full_path)
        self.table = table
        self.loader: MasterExportYamlLoader = loader
        self._cache = None
        self._list_cache = None

    @property
    def table_name(self):
        return self.table.table_name

    def iter(self):
        return (d for d in self.data)

    def find(self, hash_val: any, range_val: any = None) -> Dict[str, any]:
        hash_key = self.table.hash_key
        range_key = self.table.range_key

        if self._cache is None:
            self._cache = {}
            for rec in self.data:
                if range_key is None:
                    cache_key = (rec[hash_key.column_name],)
                else:
                    cache_key = (rec[hash_key.column_name], rec[range_key.column_name])

                self._cache[cache_key] = rec

        if range_key is None:
            find_key = (hash_val,)
        else:
            find_key = (hash_val, range_val)

        return self._cache[find_key]

    def find_list(self, hash_val: any) -> List[Dict[str, any]]:
        hash_key = self.table.hash_key

        if self._list_cache is None:
            self._list_cache = {}
            for rec in self.data:
                list_cache_key = rec[hash_key.column_name]
                self._list_cache.setdefault(list_cache_key, [])
                self._list_cache[list_cache_key].append(rec)

        return self._list_cache[hash_val]

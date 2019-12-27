from asyncio import futures
from concurrent import futures
from logging import getLogger
from pathlib import Path
from typing import Dict

from hatsudenki.packages.master.model import MasterModelSolo

_logger = getLogger(__name__)


def _build_inner(kls, yaml_dir_path: Path, cache_dir_path: Path):
    kls._load_from_yaml(yaml_dir_path, cache_dir_path, force=True)
    print(f'{kls.__name__} OK!!')


class MasterTableManager(object):
    _all_tables: Dict[str, MasterModelSolo] = {}
    _base_path: Path = None
    _dump_dir: Path = None

    @classmethod
    def register(cls, tbl):
        tbl_name = tbl.Meta.table_name
        if tbl_name is None:
            return

        if tbl_name in cls._all_tables:
            return

        cls._all_tables[tbl_name] = tbl

    @classmethod
    def get_by_table_name(cls, table_name: str):
        return cls._all_tables[table_name]

    @classmethod
    def iter_masters(cls):
        return cls._all_tables.items()

    @classmethod
    def all_load_from_yaml(cls, yaml_dir_path: Path, cache_dir_path: Path, force=False):
        _logger.info(f'load master from local_yaml {yaml_dir_path} CACHE=[{cache_dir_path}]')
        for k, t in cls._all_tables.items():
            t._load_from_yaml(yaml_dir_path, cache_dir_path, force=force)

    @classmethod
    def setup(cls, yml_path: Path):
        cls._base_path = yml_path

    @classmethod
    def _build_all(cls, yaml_dir_path: Path, cache_dir_path: Path, worker=2):
        with futures.ProcessPoolExecutor(max_workers=worker) as executor:
            l = []
            for k, t in cls._all_tables.items():
                print(k)
                l.append(executor.submit(_build_inner, t, yaml_dir_path, cache_dir_path))
            _ = futures.as_completed(l)

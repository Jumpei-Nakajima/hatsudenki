from logging import getLogger
from typing import Dict, Union

from hatsudenki.packages.cache.inmemory.table import InMemoryTableSolo, InMemoryTableMulti, SyncInMemoryTableSolo, \
    SyncInMemoryTableMulti

_logger = getLogger(__name__)


class InMemoryTableManager(object):
    _all_tables: Dict[
        str, Union[InMemoryTableSolo, InMemoryTableMulti, SyncInMemoryTableSolo, SyncInMemoryTableMulti]] = {}

    @classmethod
    def register(cls, tbl):
        tbl_name = tbl.Meta.table_name
        if tbl_name is None:
            _logger.warning(f'table name not set. {tbl}')
            return

        if tbl_name in cls._all_tables:
            _logger.critical(f'table name duplicate! {tbl_name}')
            return
        _logger.info(f'register in-memory table {tbl_name}')
        cls._all_tables[tbl_name] = tbl

    @classmethod
    def get_by_table_name(cls, table_name: str):
        return cls._all_tables[table_name]

    @classmethod
    def iter(cls):
        return cls._all_tables.items()

    @classmethod
    def invalidate_all(cls):
        for k, t in cls.iter():
            t.invalidate()

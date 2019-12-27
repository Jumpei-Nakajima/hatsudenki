from logging import getLogger
from typing import TypeVar, Type

import dill

from hatsudenki.packages.table.index import PrimaryIndex

T = TypeVar('T')
_logger = getLogger(__name__)


class InOutCacheBaseTable(object):
    class Meta:
        table_name: str = ''
        index: PrimaryIndex = None

    def serialize(self):
        return dill.dumps(self)

    @classmethod
    def deserialize(cls: Type[T], b: any) -> T:
        return dill.loads(b)

    @classmethod
    def get_table_name(cls):
        return cls.Meta.table_name

    @classmethod
    def get_hash_key(cls):
        return cls.Meta.index.hash_key

    @classmethod
    def get_range_key(cls):
        return cls.Meta.index.range_key

    @classmethod
    def _gen_key(cls, hash_val, range_val=None):
        k = f'{cls.get_table_name()}:{str(hash_val)}'

        if range_val:
            k += '--' + str(range_val)
        return k

    def _get_key(self):
        cls = self.__class__
        hk = cls.get_hash_key()
        rk = cls.get_range_key()

        hv = getattr(self, hk)
        rv = getattr(self, rk, None) if rk else None
        return cls._gen_key(hv, rv)

    async def save(self):
        pass

    @classmethod
    async def get(cls: Type[T], hash_value, range_value=None) -> T:
        v = await cls._get(hash_value, range_value)
        if v is None:
            return None
        return cls.deserialize(v)

    @classmethod
    async def _get(cls, hash_key, range_key=None) -> bytes:
        pass

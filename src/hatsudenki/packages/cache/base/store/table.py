from logging import getLogger
from typing import TypeVar, Type

import dill

T = TypeVar('T')
_logger = getLogger(__name__)


class StoreCacheBaseTable(object):
    class Meta:
        table_name: str = ''

    def serialize(self):
        return dill.dumps(self)

    @classmethod
    def deserialize(cls: Type[T], b: any) -> T:
        return dill.loads(b)

    @classmethod
    def _gen_key(cls, hash_value, range_value):
        k = f'{cls.get_table_name()}:{str(hash_value)}'

        if range_value:
            k += '--' + str(range_value)
        return k

    @classmethod
    def get_table_name(cls):
        return cls.Meta.table_name

    @classmethod
    async def build(cls, hash_value, range_value=None):
        pass

    @classmethod
    async def _store(cls, hash_value, range_value, data: bytes):
        pass

    @classmethod
    async def get(cls: Type[T], hash_value, range_value) -> T:
        v = await cls._get(hash_value, range_value)
        if v is None:
            _logger.info(f'setup {cls.Meta.table_name}')
            r = await cls.build(hash_value, range_value)
            await cls._store(hash_value, range_value, r.serialize())
            return r
        return cls.deserialize(v)

    @classmethod
    async def _get(cls, hash_key, range_key) -> bytes:
        pass

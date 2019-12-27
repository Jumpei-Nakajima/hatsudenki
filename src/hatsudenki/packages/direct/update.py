from typing import Type, TypeVar

from hatsudenki.packages.client import HatsudenkiClient
from hatsudenki.packages.expression.condition import ConditionExpression
from hatsudenki.packages.expression.update import UpdateExpression
from hatsudenki.packages.table.multi import MultiHatsudenkiTable
from hatsudenki.packages.table.solo import SoloHatsudenkiTable

Table = TypeVar('Table', SoloHatsudenkiTable, MultiHatsudenkiTable)


class DirectUpdate(object):

    def __init__(self, model: Type[SoloHatsudenkiTable]):
        self.model = model
        self.exp = UpdateExpression()
        self.cond = ConditionExpression()
        self.key = None

    def set_key_raw(self, key):
        self.key = key

    def set_key(self, hash_val, range_val):
        self.key = self.model.get_serialized_key(hash_val, range_val)

    def set_key_direct(self, **vals):
        self.key = {}

        ks = self.model.get_primary_key_names()
        for k, v in vals.items():
            if k in ks:
                self.key[k] = self.model.get_field_class(k).serialize(v)
            else:
                Exception(f'{k} is not hash_key or range_key')

    async def exec(self):
        await HatsudenkiClient.update_item(
            self.model.get_collection_name(),
            self.key,
            self.exp,
            self.cond
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.exec()

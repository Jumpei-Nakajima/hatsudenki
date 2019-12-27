from typing import Type, Union

from hatsudenki.packages.expression.condition import KeyConditionExpression, FilterConditionExpression
from hatsudenki.packages.table.multi import MultiHatsudenkiTable
from hatsudenki.packages.table.solo import SoloHatsudenkiTable


class DirectFind(object):
    def __init__(self, model: Union[Type[SoloHatsudenkiTable], Type[MultiHatsudenkiTable]]):
        self.model = model
        self.cond = KeyConditionExpression()
        self.filter = FilterConditionExpression()
        self.limit = 0

    def set_key(self, hash_val, range_val):
        with self.cond:
            self.cond.equal(self.model.get_hash_key_name(), hash_val)
            if range_val is not None:
                self.cond.op_and()
                self.cond.equal(self.model.get_range_key_name(), range_val)

    def set_key_direct(self, **vals):
        f = False
        with self.cond:
            for k, v in vals.items():
                if not f:
                    self.cond.op_and()
                fc = self.model.get_field_class(k)
                if fc is None:
                    raise Exception(f'invalid key {k}')
                self.cond.equal(k, fc.serialize(v), raw=True)

    def set_limit(self, limit: int):
        self.limit = limit

    async def exec(self):
        pass
        # res = await self.model.query(self.cond, self.filter, limit=self.limit)
        # return res

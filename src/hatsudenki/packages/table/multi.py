from typing import List, Type, TypeVar

from hatsudenki.define.config import CURSOR_SEPARATOR
from hatsudenki.packages.expression.condition import ConditionExpression
from hatsudenki.packages.table.define import TableType
from hatsudenki.packages.table.solo import SoloHatsudenkiTable

T = TypeVar('T')


class MultiHatsudenkiTable(SoloHatsudenkiTable):

    @classmethod
    def get_table_type(cls):
        if cls.Meta.is_root:
            return TableType.RootTable
        return TableType.SingleMultiTable

    @classmethod
    def _is_allow_modify(cls, key: str):
        t = super()._is_allow_modify(key)
        if t:
            return cls.get_range_key_name() != key
        return False

    @property
    def range_key_name(self):
        return self.__class__.get_range_key_name()

    @classmethod
    def get_range_key_name(cls):
        p = cls.get_primary_index()
        return p.range_key

    @property
    def range_value(self):
        """
        ハッシュキーの値を取得

        :return: ハッシュキーの値（型はモデルに依存）
        """
        return getattr(self, self.range_key_name)

    @property
    def one_cursor(self):
        r = self.__class__.get_field_class(self.range_key_name)
        k = super().one_cursor + CURSOR_SEPARATOR + r.to_string(self.range_value)
        return k

    @property
    def serialized_key(self):
        c = self.__class__
        hk = c._hash_key_name
        rk = c._range_key_name
        d = self.__dict__

        return {
            hk: c._serializer[hk](d[hk]),
            rk: c._serializer[rk](d[rk])
        }

    @classmethod
    def not_exist_condition(cls, cond: ConditionExpression = None):
        p = super().not_exist_condition(cond)
        p.op_and()
        p.attribute_not_exists(cls.get_range_key_name())
        return p

    @classmethod
    def exist_condition(cls, cond: ConditionExpression = None):
        p = super().exist_condition(cond)
        p.op_and()
        p.attribute_exists(cls.get_range_key_name())
        return p

    async def reget(self):
        c = await self.__class__.get(self.hash_value, self.range_value)
        self.copy(c)

    @classmethod
    def get_serialized_key(cls, hash_val: any, range_val: any = None):
        if range_val is None:
            raise Exception('invalid key')
        hash_key = cls._hash_key_name
        range_key = cls._range_key_name
        return {
            hash_key: cls._serializer[hash_key](hash_val),
            range_key: cls._serializer[range_key](range_val)
        }

    @property
    def many_cursor(self):
        return f'{self.hash_value}//{self.range_value}'

    def set_range_key(self, val):
        """
        ハッシュキーの値をセットする
        このメソッドを使わずにハッシュキーを書き換えると例外が発生する

        :param val: セットする値（型はモデルに依存）
        :return: None
        """
        rk = self.get_range_key_name()
        f = self.__class__.get_field_class(rk)
        self.force_set_key(rk, f.get_data(val))

    @classmethod
    async def query_by_cursor(cls: Type[T], cursor: str) -> T:

        rp = cursor.split(CURSOR_SEPARATOR)

        # k = cls.get_serialized_key(*rp[:2])
        return await cls.get(*rp[:2])

    @classmethod
    async def query_list_by_cursor(cls: Type[T], cursor: str) -> List[T]:
        return await cls.query_list({
            cls.get_hash_key_name(): cursor
        })

    @classmethod
    def get_range_field_class(cls):
        return cls.get_field_class(cls.get_range_key_name())

    @classmethod
    def get_primary_key_names(cls):
        return [cls.get_hash_key_name(), cls.get_range_key_name()]

    async def remove(self):
        await self.__class__.delete(self.hash_value, self.range_value)

    @classmethod
    def _check_key(cls, hash_val: any, range_val: any):
        if range_val is None:
            raise Exception('invalid key')

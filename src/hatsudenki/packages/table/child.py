from typing import Type, TypeVar, Tuple, List

from hatsudenki.define.config import TAG_SEPARATOR, CURSOR_SEPARATOR
from hatsudenki.packages.field.base import BaseHatsudenkiField
from hatsudenki.packages.table.define import TableType
from hatsudenki.packages.table.multi import MultiHatsudenkiTable

T = TypeVar("T")


class ChildMultiHatsudenkiTable(MultiHatsudenkiTable):
    class Meta(MultiHatsudenkiTable.Meta):
        key_alias_name = ''
        key_alias_type: BaseHatsudenkiField = None
        tag_name = ''

    @classmethod
    def get_table_type(cls):
        return TableType.ChildTable

    @classmethod
    def get_alias_key(cls):
        return cls.Meta.key_alias_name

    @property
    def alias_value(self):
        return getattr(self, self.__class__.get_alias_key())

    def set_alias_key(self, val):
        a = self.resolve_alias(val)
        self.set_range_key(a)

    @classmethod
    def get_alias_key_type(cls):
        return cls.Meta.key_alias_type

    @classmethod
    def serialize_alias_key(cls, val):
        return cls.get_alias_key_type().serialize(val)

    @classmethod
    def get_tag_name(cls):
        return cls.Meta.tag_name

    @classmethod
    def generate_range_key_str(cls, val=None):
        f = cls.get_alias_key_type()
        return f'{cls.get_tag_name()}{TAG_SEPARATOR}{f.to_string(val)}'

    @classmethod
    def resolve_alias(cls, val):
        if val is None:
            raise Exception('invalid key.')
        r = cls.generate_range_key_str(val)
        return r

    @classmethod
    def query_parse(cls, query_dict: dict):
        # childテーブルはkindを自動設定する

        # 使用するインデックスを判定
        ak = cls.get_alias_key()
        rk = cls.get_range_key_name()
        hash_key, range_key, op, use_index = cls._find_index_by_query_dict(query_dict)
        if range_key == ak:
            if op is None:
                # エイリアスキーが明示的に指定されている
                query_dict[rk] = cls.resolve_alias(query_dict[ak])
                # エイリアスキーは実在しないので消す
                del query_dict[ak]
            elif op == 'beginsWith':
                # エイリアスキーが指定され、且つオペレータが指定されている
                k = ak + '__' + op
                v = query_dict[k]
                # エイリアスキーは実在しないので消す
                query_dict[rk + '__' + op] = cls.resolve_alias(v)
                del query_dict[ak + '__' + op]
            else:
                # ごめんね、まだできてないんだ
                raise Exception(f'alias_key operation [{op}] is not supported yet.')

        elif range_key is None and hash_key == cls.get_hash_key_name():
            # レンジキーが指定されていない且つ、ハッシュキーがプライマリーのもの（GSIでない）場合は
            # 暗黙的に本来のレンジキーをTAG_NAMEの前方一致検索とする
            query_dict[cls.get_range_key_name() + '__beginsWith'] = f'{cls.get_tag_name()}{TAG_SEPARATOR}'

        return super().query_parse(query_dict)

    @property
    def many_cursor(self):
        h = self.get_hash_field_class()
        return h.to_string(self.hash_value)

    @property
    def one_cursor(self):
        h = self.get_hash_field_class()
        t = self.get_alias_key_type()
        return f'{h.to_string(self.hash_value)}{CURSOR_SEPARATOR}{t.to_string(self.alias_value)}'

    @classmethod
    async def get_or_create(cls: Type[T], hash_val: any, alias_val: any = None) -> Tuple[bool, T]:
        r = cls.resolve_alias(alias_val)
        return await super().get_or_create(hash_val, r)

    @classmethod
    async def get(cls: Type[T], hash_val: any, alias_val: any = None, prj_exp: List[str] = None) -> T:
        range_val = cls.resolve_alias(alias_val)
        return await super().get(hash_val, range_val, prj_exp)

    @classmethod
    async def delete(cls, hash_val: any, alias_val: any = None):
        range_val = cls.resolve_alias(alias_val)
        return await super().delete(hash_val, range_val)

    async def remove(self):
        await self.__class__.delete(self.hash_value, self.alias_value)

    async def reget(self):
        c = await self.__class__.get(self.hash_value, self.alias_value)
        self.copy(c)

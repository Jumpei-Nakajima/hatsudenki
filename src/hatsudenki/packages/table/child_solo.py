from typing import List

from hatsudenki.packages.table.child import ChildMultiHatsudenkiTable
from hatsudenki.packages.table.define import TableType


class ChildSoloHatsudenkiTable(ChildMultiHatsudenkiTable):

    @classmethod
    def get_table_type(cls):
        return TableType.SingleSoloTable

    @property
    def many_cursor(self):
        raise Exception('Invalid operation.')

    @classmethod
    def resolve_alias(cls, val):
        if val is not None:
            raise Exception('invalid key.')
        # childSoloテーブルは常に固定値
        return cls.get_tag_name()

    @property
    def alias_value(self):
        # childSoloテーブルは常にNone
        return None

    @classmethod
    async def get_iter(cls, hash_val: any, limit=0, prj_exp: List[str] = None):
        raise Exception('invalid operation')

    @classmethod
    def query_parse(cls, query_dict: dict):
        hk = query_dict.get(cls.get_hash_key_name(), None)
        if hk is not None:
            # Hashキーが明示的に指定されている場合はプライマリキーを使用していると仮定する
            ak = query_dict.get(cls.get_alias_key(), None)
            if ak is not None:
                # SoloテーブルはAliasキーを明示的に指定できない
                raise Exception('invalid key.')
            else:
                # SoloテーブルのAliasキーは固定で設定される
                query_dict[cls.get_range_key_name()] = cls.get_tag_name()

        return super().query_parse(query_dict)

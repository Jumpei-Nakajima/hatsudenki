from enum import Enum
from typing import List, Union

from hatsudenki.define.config import KEYS_SEPARATOR
from hatsudenki.packages.field.base import BaseHatsudenkiField


class MasterKeysField(BaseHatsudenkiField['MasterKeysField.Value']):
    TypeStr = 'S'

    class Value(object):
        def __init__(self, name: str, label_list: List[str], parent=None):
            from hatsudenki.packages.table.solo import SoloHatsudenkiTable

            self.parent: SoloHatsudenkiTable = parent
            self.label_list = label_list
            self.name = name

        def is_empty(self):
            return not all(self.label_list)

        def to_string(self):
            return KEYS_SEPARATOR.join(self.label_list)

        def update_key(self):
            if self.parent is None:
                raise Exception('invalid operation. update_key')
            setattr(self.parent, self.name, self)

        def clear(self):
            self.label_list = [
                None
                for _ in range(len(self.label_list))
            ]
            self.update_key()

        def __str__(self):
            return self.to_string()

    def __init__(self, split_num: int, **kwargs):
        super().__init__(**kwargs)
        self.split_num = split_num

    def get_data(self, val: Union[str, list, None, Value], parent=None) -> Value:
        if type(val) is str:
            val = val.split(KEYS_SEPARATOR, self.split_num)
        elif type(val) is list:
            val = self._list_to_cursor(val)
        elif val is None:
            val = [None] * self.split_num
        elif type(val) is self.__class__.Value:
            return val
        else:
            raise Exception(f'invalid keys. type={type(val)}')
        return self.__class__.Value(self.name, val, parent)

    def _list_to_cursor(self, data_list: list):
        # クエリに設定する際に数が足りない要求が来る場合があるので
        if len(data_list) > self.split_num:
            raise Exception(f'data_list size unmatch. recv={len(data_list)} self={self.split_num}')
        ret = []
        for d in data_list:
            if hasattr(d, 'one_cursor'):
                ret.append(d.one_cursor)
            elif hasattr(d, 'value'):
                ret.append(str(d.value))
            else:
                raise Exception('invalid list.')
        return ret

    @classmethod
    def is_empty(cls, value):
        if value is None:
            return True
        if type(value) is list:
            return len(value) is 0
        return value.is_empty()

    def to_string(self, val: Union[str, list, Value]):
        if val is None:
            return None
        if isinstance(val, MasterKeysField.Value):
            return val.to_string()
        if type(val) is str:
            return self.get_data(val).to_string()
        if type(val) is list:
            l = len(val)
            if l is 0:
                return None
            if l > self.split_num:
                raise Exception(f'invalid keys. {val}')

            k = []
            for idx, m in enumerate(val):
                if isinstance(m, Enum):
                    k.append(str(m.value))
                else:
                    k.append(m.one_cursor)

            r = KEYS_SEPARATOR.join(k)
            if l < self.split_num:
                return r + KEYS_SEPARATOR
            else:
                return r
        raise Exception(f'invalid key type {val}')

    def serialize(self, value, table=None):
        if self.__class__.is_empty(value):
            return None
        rv = self.get_data(value, table)
        return {'S': rv.to_string()}

    def deserialize(self, value, table=None):
        s = super().deserialize(value, table)
        return self.get_data(s, table)

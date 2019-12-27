from typing import Generic, TypeVar, Type, Optional

from hatsudenki.packages.expression.update import UpdateExpression
from hatsudenki.packages.field.base import BaseHatsudenkiField
from hatsudenki.packages.marked import MarkedObjectList
from hatsudenki.packages.marked import MarkedObjectMap

T = TypeVar('T')


class DictField(BaseHatsudenkiField[dict]):
    PythonType = dict
    TypeStr = 'M'
    TypeName = 'Dict'


class MapField(BaseHatsudenkiField[MarkedObjectMap[T]], Generic[T]):
    """
    マップ（Dictionary）フィールドクラス
    """
    PythonType = dict

    TypeStr = 'M'
    TypeName = 'Map'
    IsScalar = False

    def __init__(self, value_type: Type[T], *, name=None, default=None, ttl=False):
        super().__init__(name=name, default=default, ttl=ttl)
        self.value_type = value_type

    def get_data_from_dict(self, v: dict, table=None) -> MarkedObjectMap[T]:
        return super().get_data_from_dict(v, table)

    def get_data(self, val, table=None) -> Optional[T]:
        if self.is_empty(val):
            return MarkedObjectMap(self.name, self.value_type, table)

        if isinstance(val, self.value_type):
            return MarkedObjectMap(self.name, self.value_type, table, val)
        if isinstance(val, MarkedObjectMap):
            return val

        if type(val) is dict:
            return MarkedObjectMap(self.name, self.value_type, table, val)

        raise Exception(f'invalid value given. {val}')

    @classmethod
    def is_empty(cls, value: MarkedObjectMap[T]):
        if value is None:
            return True
        return value.is_empty()

    def serialize(self, value: MarkedObjectMap[T], table=None):
        if self.is_empty(value):
            return None

        return value.serialized_value

    def deserialize(self, value, table=None) -> MarkedObjectMap:
        if value is None:
            return MarkedObjectMap(self.name, self.value_type, value, table)
        return MarkedObjectMap.deserialize(self.name, self.value_type, value, table)

    def build_update_expression(self, upd: UpdateExpression, now_value: MarkedObjectList[T]):
        now_value.build_update_expression(upd)


class ListField(BaseHatsudenkiField[MarkedObjectList[T]], Generic[T]):
    """
    リスト（配列）フィールドクラス
    """
    PythonType = list

    TypeStr = 'L'
    TypeName = 'List'

    # オブジェクト型である
    IsScalar = False

    def __init__(self, value_type: Type[T], *, name=None, default=None, ttl=False):
        super().__init__(name=name, default=default, ttl=ttl)
        self.value_type = value_type

    @classmethod
    def is_empty(cls, value: MarkedObjectList[T]):
        return value is None or len(value) is 0

    def get_data_from_dict(self, v: dict, table=None) -> MarkedObjectList[T]:
        return super().get_data_from_dict(v, table)

    def get_data(self, val, table=None) -> MarkedObjectList[T]:
        if self.is_empty(val):
            return MarkedObjectList(self.name, self.value_type, table)

        if type(val) is list or isinstance(val, MarkedObjectList):
            # オブジェクトのリストである可能性
            return MarkedObjectList(self.name, self.value_type, table, val)

        raise Exception(f'invalid value given. {val}')

    def serialize(self, value: MarkedObjectList[T], table=None):
        if self.__class__.is_empty(value):
            return None

        return value.serialized_value

    def deserialize(self, value, table=None) -> MarkedObjectList[T]:
        if value is None:
            return MarkedObjectList(self.name, self.value_type, table)
        return MarkedObjectList.deserialize(self.name, self.value_type, value, table)

    def build_update_expression(self, upd: UpdateExpression, now_value: MarkedObjectList[T]):
        now_value.build_update_expression(upd)

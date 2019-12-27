from typing import Generic, TypeVar, Type, Optional

from hatsudenki.packages.expression.update import UpdateExpression
from hatsudenki.packages.field import BaseHatsudenkiField
from hatsudenki.packages.marked import MarkedObjectDict

T = TypeVar('T')


class DictMapField(BaseHatsudenkiField[MarkedObjectDict[T]], Generic[T]):
    PythonType = dict
    TypeStr = 'M'
    TypeName = 'DictMap'

    IsScalar = False

    def __init__(self, value_type: Type[T], *, name=None, default=None, ttl=False):
        super().__init__(name=name, default=default, ttl=ttl)
        self.value_type = value_type

    def get_data_from_dict(self, v: dict, table=None) -> MarkedObjectDict[T]:
        return super().get_data_from_dict(v, table)

    def get_data(self, val, table=None) -> Optional[T]:
        if self.is_empty(val):
            return MarkedObjectDict(self.name, self.value_type, table)

        if isinstance(val, self.value_type):
            return MarkedObjectDict(self.name, self.value_type, table, val)
        if isinstance(val, MarkedObjectDict):
            return val

        if type(val) is dict:
            return MarkedObjectDict(self.name, self.value_type, table, val)

        raise Exception(f'invalid value given. {val}')

    @classmethod
    def is_empty(cls, value: MarkedObjectDict[T]):
        if value is None:
            return True
        return value.is_empty()

    def serialize(self, value: MarkedObjectDict[T], table=None):
        if self.is_empty(value):
            return None

        return value.serialized_value

    def deserialize(self, value, table=None) -> MarkedObjectDict[T]:
        if value is None:
            return MarkedObjectDict(self.name, self.value_type, table, None)

        return MarkedObjectDict.deserialize(self.name, self.value_type, value, table)

    def build_update_expression(self, upd: UpdateExpression, now_value):
        now_value.build_update_expression(upd)

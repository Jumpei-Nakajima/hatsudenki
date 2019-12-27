from datetime import datetime
from logging import getLogger
from typing import Optional, Type, TypeVar
from uuid import UUID

from hatsudenki.packages.field.base import BaseHatsudenkiField

_logger = getLogger(__name__)

T = TypeVar('T')


class UUIDField(BaseHatsudenkiField[UUID]):
    PythonType = UUID

    TypeStr = 'S'
    TypeName = 'UUID'

    def serialize(self, value, table=None):
        if type(value) is str:
            u = UUID(hex=value)
            return {'S': u.hex}

        if self.__class__.is_empty(value):
            return None
        return {'S': value.hex}

    def deserialize(self, value, table=None):
        if value is None:
            return self.default_value
        return UUID(hex=value['S'])

    def get_data(self, val, parent=None) -> Optional[UUID]:
        if val is None:
            return None

        if type(val) is UUID:
            return val
        if type(val) is str:
            return UUID(hex=val)

    def to_string(self, val: UUID):
        return str(val.hex)


class DateField(BaseHatsudenkiField[datetime]):
    TypeStr = 'N'
    PythonType = datetime
    TypeName = 'Date'

    def serialize(self, value, table=None):
        if type(value) is str:
            d = datetime.fromtimestamp(int(value))
            return {'N': str(int(d.timestamp()))}

        if self.is_empty(value):
            return None
        return {'N': str(int(value.timestamp()))}

    def deserialize(self, value, table=None):
        if value is None:
            return self.default_value
        return datetime.fromtimestamp(int(value['N']))

    def to_string(self, val: datetime):
        return str(int(val.timestamp()))

    def get_data(self, val, table=None):
        if type(val) is datetime:
            return val
        if type(val) is int:
            return datetime.fromtimestamp(val)
        if type(val) is str:
            return datetime.fromisoformat(val)


class CreateDateField(DateField):
    pass


class UpdateDateField(DateField):
    pass


class EnumField(BaseHatsudenkiField[int]):
    TypeStr = 'N'
    PythonType = int
    TypeName = 'Enum'

    def __init__(self, to: Type[T], **kwargs):
        super().__init__(**kwargs)
        self.to = to

    def serialize(self, value: T, table=None):
        if type(value) == self.to:
            return {'N': str(value.value)}
        elif type(value) == int:
            return {'N': str(value)}
        elif self.is_empty(value):
            return None

    def deserialize(self, value, table=None):
        try:
            return self.to(int(value['N']))
        except ValueError as e:
            _logger.warning(f'Enum value is not exist. table: {table}, value: {int(value["N"])}')
            return None

    def to_string(self, val):
        return str(int(val.value))

    def get_data(self, val, table=None):
        if type(val) == self.to:
            return val
        elif type(val) == int:
            return self.to(val)
        elif self.is_empty(val):
            return None

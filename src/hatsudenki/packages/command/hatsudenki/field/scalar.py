from typing import Optional, List

from hatsudenki.packages.command.hatsudenki.field import field, HatsudenkiFieldBase


@field('string', 'str')
class StringField(HatsudenkiFieldBase):
    PythonStr = 'field.StringField'

    @property
    def default_value(self):
        d = super().default_value
        return f"'{d}'" if d is not None else None

    @property
    def gql_filters(self) -> Optional[List[str]]:
        return ['beginsWith']


@field('number', 'int')
class NumberField(HatsudenkiFieldBase):
    PythonStr = 'field.NumberField'

    @property
    def gql_filters(self) -> Optional[List[str]]:
        return ['gt', 'gte', 'lte', 'lt']


@field('set_number', 'set')
class NumberSetField(HatsudenkiFieldBase):
    PythonStr = 'field.NumberSetField'


@field('set_string', 'set')
class StringSetField(HatsudenkiFieldBase):
    PythonStr = 'field.StringSetField'


@field('bool', 'bool')
class BoolField(HatsudenkiFieldBase):
    TypeStr = 'bool'
    PythonStr = 'field.BoolField'


@field('binary', 'bytes')
class BinaryField(HatsudenkiFieldBase):
    PythonStr = 'field.BinaryField'

    @property
    def default_value(self):
        return None

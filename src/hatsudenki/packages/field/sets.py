from hatsudenki.packages.expression.update import UpdateExpression
from hatsudenki.packages.field.base import BaseHatsudenkiField


class StringSetField(BaseHatsudenkiField[set]):
    """
    文字列セットフィールドクラス。
    """

    TypeStr = 'SS'
    ValueType = str
    TypeName = 'StringSet'

    def is_empty(cls, value):
        return value is None or len(value) is 0

    @classmethod
    def add_expression(cls, name: str, new_value, old_value, update: UpdateExpression):
        update.increment(name, cls.serialize(new_value - old_value), raw=True)


class NumberSetField(BaseHatsudenkiField[set]):
    """
    数値セットフィールドクラス。重複した値は格納できない。
    """
    PythonType = set

    TypeStr = 'NS'
    ValueType = int
    TypeName = 'NumberSet'

    @classmethod
    def add_expression(cls, name: str, new_value, old_value, update: UpdateExpression):
        update.increment(name, cls.serialize(new_value - old_value), raw=True)


class BinarySetField(BaseHatsudenkiField[set]):
    """
    バイナリセットフィールドクラス。重複した値は格納できない。
    """
    PythonType = set

    TypeStr = 'BS'
    ValueType = bytes
    TypeName = 'BinarySet'

    @classmethod
    def is_empty(cls, value):
        return value is None or len(value) is 0

    @classmethod
    def add_expression(cls, name: str, new_value, old_value, update: UpdateExpression):
        update.increment(name, cls.serialize(new_value - old_value), raw=True)

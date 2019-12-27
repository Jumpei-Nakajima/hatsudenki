from hatsudenki.packages.field import T
from hatsudenki.packages.field.base import BaseHatsudenkiField


class StringField(BaseHatsudenkiField[str]):
    """
    文字列フィールドクラス
    """
    PythonType = str
    TypeStr = 'S'
    TypeName = 'String'

    @classmethod
    def is_empty(cls, value):
        return value is None or len(value) is 0

    def deserialize(self, value, table=None):
        if value is None:
            return self._default_value
        return value['S']

    def serialize(self, value: T, table=None):
        if value is None or len(value) is 0:
            return None

        return {'S': value}


class NumberField(BaseHatsudenkiField[int]):
    """
    数値フィールドクラス。符号はどっちも入る。少数は駄目
    """
    PythonType = int
    TypeStr = 'N'
    TypeName = 'Integer'

    # @classmethod
    # def add_expression(cls, name: str, new_value, old_value, update: UpdateExpression):
    #     if old_value is None:
    #         update.add()
    #     update.increment(name, cls.serialize(new_value - old_value, None), raw=True)

    def deserialize(self, value, table=None):
        if value is None:
            return self._default_value

        return int(value['N'])

    def serialize(self, value: T, table=None):
        if value is None:
            return None

        return {'N': f'{value}'}


class BinaryField(BaseHatsudenkiField[bytes]):
    """
    バイナリフィールドクラス
    """
    PythonType = bytes
    TypeStr = 'B'
    TypeName = 'Binary'

    @classmethod
    def is_empty(cls, value):
        return value is None or len(value) is 0

    def deserialize(self, value, table=None):
        if value is None:
            return self._default_value

        return value['B']

    def serialize(self, value: T, table=None):
        if value is None or len(value) is 0:
            return None

        return {'B': value}


class BoolField(BaseHatsudenkiField[bool]):
    """
    ブーリアンフィールドクラス
    """
    PythonType = bool
    TypeStr = 'BOOL'
    TypeName = 'Boolean'

    def deserialize(self, value, table=None):
        if value is None:
            return self._default_value

        return value['BOOL']

    def serialize(self, value: T, table=None):
        if value is None:
            return None

        return {'BOOL': value}

from typing import Generic, TypeVar

from hatsudenki.packages.expression.update import UpdateExpression
from hatsudenki.packages.field.util import primal_deserializer, primal_serializer

T = TypeVar('T')


class BaseHatsudenkiField(Generic[T]):
    """
    DynamoDBのアイテムフィールドクラス
    """
    PythonType = None
    TypeStr = ''
    ValueType = None
    DefaultValue = None
    TypeName = ''
    IsScalar = True

    def __init__(self, *, name=None, default=None, ttl=False):
        """
        init

        :param name: 名前
        :param default: デフォルト値
        :param ttl: ttlとして参照するか
        """
        self.is_ttl = ttl
        self._default_value = default
        self.name = name

    @classmethod
    def is_empty(cls, value):
        """
        値が空か

        :param value: 対象値
        :return: bool
        """
        return value is None

    @property
    def default_value(self):
        """
        デフォルト値

        :return: value
        """
        return self._default_value if self._default_value is not None else self.__class__.DefaultValue

    @property
    def attribute_define(self):
        """
        定義用スキーマ

        :return: dict
        """
        return {
            'AttributeName': self.name,
            'AttributeType': self.TypeStr
        }

    def key_schema(self, type_str: str):
        """
        キー定義用スキーマ

        :param type_str: 'HASH'または'RANGE'
        :return: dict
        """
        return {
            'AttributeName': self.name,
            'KeyType': type_str
        }

    def deserialize(self, value, table=None):
        """
        Pythonが解釈できる値にデシリアライズする

        :param value:
        :param table:
        :return:
        """
        if value is None:
            return self._default_value

        return primal_deserializer(value)

    def serialize(self, value: T, table=None):
        """
        DynamoDBが解釈できる値にシリアライズする

        :param value: 対象の値
        :return: DynamoDBに準拠した値情報連想配列
        """
        if self.is_empty(value):
            return None

        return primal_serializer(value)

    @classmethod
    def set_expression(cls, name: str, value, update: UpdateExpression):
        update.set(name, cls.serialize(value), raw=True)

    @classmethod
    def add_expression(cls, name: str, new_value, old_value, update: UpdateExpression):
        update.set(name, cls.serialize(new_value), raw=True)

    def __str__(self):
        return self.name

    def get_data(self, val, table=None) -> T:
        if val is None:
            return self.default_value

        if type(val) is self.PythonType:
            return val
        return self.PythonType(val)

    def get_data_from_dict(self, v: dict, table=None) -> T:
        return self.get_data(v.get(self.name, None), table)

    def dump_text(self, val):
        return f'{self.name}: {val.__repr__()}'

    def to_string(self, val):
        return str(val)

    def build_update_expression(self, upd: UpdateExpression, now_value):
        upd.set(self.name, self.serialize(now_value), raw=True)

from typing import Generic, TypeVar, Type, Union, List

from hatsudenki.packages.field import BaseHatsudenkiField, T

K = TypeVar('K')


class ReferenceMasterOneField(Generic[K], BaseHatsudenkiField['ReferenceMasterOneField.Value']):
    TypeStr = 'S'

    class Value(Generic[K]):
        def __init__(self, name: str, to: Type[K], label: str, parent=None):
            from hatsudenki.packages.table.solo import SoloHatsudenkiTable
            self.parent: SoloHatsudenkiTable = parent
            self.label = label
            self.name = name
            self.to = to

        def is_empty(self):
            return self.label is None or len(self.label) is 0

        def to_string(self):
            return self.label

        def update_key(self):
            if self.parent is None:
                return
            setattr(self.parent, self.name, self)

        @property
        def value(self):
            return self.label

        def resolved_value(self) -> K:
            return self.to.get_by_cursor(self.label)

        def connect(self, t: K):
            if t is None:
                self.label = None
            else:
                self.label = t.one_cursor
            self.update_key()

        def __str__(self):
            return self.to_string()

    def __init__(self, to: Type[K], **kwargs):
        super().__init__(**kwargs)
        self.to = to

    @property
    def to_table(self):
        return self.to

    def get_data(self, val: Union[str, K, None], parent=None) -> Value[K]:
        tp = type(val)
        if tp is str:
            val = val
        elif tp is self.to_table:
            val = val.one_cursor
        elif val is None:
            val = None
        elif tp is self.__class__.Value:
            return val
        else:
            raise Exception(f'invalid keys.{val}')
        return self.__class__.Value(self.name, self.to_table, val, parent)

    def get_data_from_dict(self, v: dict, table=None) -> Value[K]:
        return super().get_data_from_dict(v, table)

    @classmethod
    def is_empty(cls, value):
        if value is None:
            return True
        return value.is_empty()

    def to_string(self, val: Union[str, K, Value]):
        if val is None:
            return None
        if isinstance(val, ReferenceMasterOneField[K].Value):
            return val.to_string()
        if type(val) is str:
            return val
        if isinstance(val, self.to):
            return val.one_cursor
        raise Exception(f'invalid key type {val}')

    def serialize(self, value: T, table=None):
        s = value.to_string()
        if s:
            return {'S': s}
        return None

    def deserialize(self, value, table=None):
        # s = super().deserialize(value, table)
        if value:
            return self.get_data(value['S'], table)
        return self.__class__.Value(self.name, self.to_table, None, table)


class ReferenceMasterManyField(ReferenceMasterOneField[K]):
    def __init__(self, to: Type[K], **kwargs):
        super().__init__(to=to, **kwargs)

    class Value(ReferenceMasterOneField.Value, Generic[K]):
        def resolved_value(self) -> List[K]:
            return self.to.find_by_cursor(self.label)

        def connect(self, t: K):
            self.label = t.many_cursor
            self.update_key()

    def get_data(self, val: Union[str, K, None], parent=None) -> Value[K]:
        if type(val) is str:
            val = val
        elif type(val) is self.to_table:
            val = val.many_cursor
        elif val is None:
            val = None
        else:
            raise Exception('invalid keys.')
        return self.__class__.Value(self.name, self.to_table, val, parent)

    def get_data_from_dict(self, v: dict, table=None) -> Value[K]:
        return super().get_data_from_dict(v, table)


class ReferenceDynamoOneField(ReferenceMasterOneField[K]):
    TypeStr = 'S'

    class Value(ReferenceMasterOneField.Value, Generic[K]):

        async def resolved_value(self) -> K:
            return await self.to.query_by_cursor(self.label)

        def clear(self):
            self.label = None
            self.update_key()

        def connect(self, t: K):
            if t is None:
                self.clear()
            else:
                super().connect(t)

    def __init__(self, to: str, **kwargs):
        super().__init__(to=None, **kwargs)
        self.to = to

    @property
    def to_table(self):
        from hatsudenki.packages.manager.table import TableManager
        return TableManager.get_by_table_name(self.to)

    def get_data(self, val: Union[str, K, None], parent=None) -> Value[K]:
        return super().get_data(val, parent)

    def get_data_from_dict(self, v: dict, table=None) -> Value[K]:
        return super().get_data_from_dict(v, table)


class ReferenceDynamoManyField(ReferenceDynamoOneField[K]):
    TypeStr = 'S'

    class Value(ReferenceMasterOneField.Value, Generic[K]):
        async def resolved_value(self) -> List[K]:
            return await self.to.query_list_by_cursor(self.label)

        def connect(self, t: K):
            self.label = t.many_cursor
            self.update_key()

    @property
    def to_table(self):
        from hatsudenki.packages.manager.table import TableManager
        return TableManager.get_by_table_name(self.to)

    def get_data(self, val: Union[str, K, None], parent=None) -> Value[K]:
        if type(val) is str:
            val = val
        elif type(val) is self.to_table:
            val = val.many_cursor
        elif val is None:
            val = None
        else:
            raise Exception('invalid keys.')
        return self.__class__.Value(self.name, self.to_table, val, parent)

    def get_data_from_dict(self, v: dict, table=None) -> Value[K]:
        return super().get_data_from_dict(v, table)

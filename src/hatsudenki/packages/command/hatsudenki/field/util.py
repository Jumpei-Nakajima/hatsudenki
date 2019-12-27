from hatsudenki.packages.command.hatsudenki.field import HatsudenkiFieldBase

_DYNAMO_FIELDS = {}


class FieldFactory(object):
    _fields = {}

    @classmethod
    def create_field(cls, name: str, data: dict, parent) -> HatsudenkiFieldBase:
        return cls._fields[data['type']](name, data, parent)

    @classmethod
    def register(cls, field: HatsudenkiFieldBase):
        cls._fields[field.TypeStr] = field


def field(key, val_type):
    def _wrap(cls):
        _DYNAMO_FIELDS[key] = cls
        cls.TypeStr = key
        cls.PythonValueTypeStr = val_type

        FieldFactory.register(cls)
        return cls

    return _wrap

import re
from enum import Enum
from typing import Match

from hatsudenki.packages.expression.base import BaseExpression


class ExpressionMode(Enum):
    Default = 'ConditionExpression'
    Find = 'KeyConditionExpression'
    Filter = 'FilterExpression'


class KeyConditionExpression(BaseExpression):
    ParameterLabel = 'KeyConditionExpression'
    ValuePrefix = 'key'

    FilterWordMap = {
        'gt': 'greater',
        'lt': 'less',
        'gte': 'greater_than',
        'lte': 'less_than',
        'beginsWith': 'begins_with',
        'between': 'between'
    }

    FuncArgumentType = {
        'between': 'open',
    }

    def __init__(self):
        super().__init__()
        self.operations = ''
        self.val_num = 0

    def get_operation_by_word(self, word: str, *args, **kwargs):
        op = getattr(self, self.__class__.FilterWordMap.get(word, word), None)
        if op is None:
            Exception(f'invalid filter operation word. {word}')
        op(*args, **kwargs)

    def parse_key_value(self, key: str, value):
        ks = key.split('__')
        if len(ks) >= 2:
            op_func_name = self.__class__.FilterWordMap.get(ks[1], None)
            if op_func_name is None:
                Exception(f'invalid filter operation word. {ks[1]}')
            op_func = getattr(self, op_func_name)
            arg_type = self.__class__.FuncArgumentType.get(op_func_name)
            if arg_type == 'open':
                op_func(ks[0], *value)
            else:
                op_func(ks[0], value)
        else:
            self.equal(key, value)

    def is_empty(self):
        return len(self.operations) is 0

    def _make_func(self, op: str, key: str):
        k = self._register_key(key)
        self.operations += f'{op}({k})'

    def attribute_exists(self, key: str):
        self._make_func('attribute_exists', key)

    def attribute_not_exists(self, key: str):
        self._make_func('attribute_not_exists', key)

    def _make_comp(self, op: str, key: str, val: dict, raw=False):
        k = self._register_key(key)
        v = self._register_value(val, raw)
        op = f'{k} {op} {v}'
        self.operations += op

    def equal(self, key: str, val: any, raw=False):
        self._make_comp('=', key, val, raw)

    def greater(self, key: str, val: dict, raw=False):
        self._make_comp('>', key, val, raw)

    def greater_than(self, key: str, val: dict, raw=False):
        self._make_comp('>=', key, val, raw)

    def less(self, key: str, val: dict, raw=False):
        self._make_comp('<', key, val, raw)

    def less_than(self, key: str, val: dict, raw=False):
        self._make_comp('<=', key, val, raw)

    def between(self, key: str, lo: any, hi: any, raw=False):
        k = self._register_key(key)
        l = self._register_value(lo, raw)
        h = self._register_value(hi, raw)
        self.operations += f'{k} BETWEEN {l} AND {h}'

    def begins_with(self, key: str, val: str, raw=False):
        k = self._register_key(key)
        v = self._register_value(val, raw)
        self.operations += f'begins_with({k} ,  {v})'

    def op_and(self):
        if self.is_empty():
            return self
        self.operations = f'{self.operations} AND '
        return self

    def op_or(self):
        self.operations = f'{self.operations} OR '
        return self

    def __enter__(self):
        self.operations += '('

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.operations += ')'

    @property
    def expression(self):
        return self.operations

    @classmethod
    def dump(cls, data: dict):
        d = data[cls.ParameterLabel]
        k = data[cls.ParameterAttributeName.Names.value]
        v = data[cls.ParameterAttributeName.Values.value]

        def _(m: Match):
            return k[m.group()]

        def __(m: Match):
            return str(v[m.group()])

        p = re.sub(r'#key_\w+', _, d)
        return re.sub(r':key_\w+', __, p)


class ConditionExpression(KeyConditionExpression):
    ParameterLabel = 'ConditionExpression'
    ValuePrefix = 'get'

    FilterWordMap = {
        'in': 'in_values',
        'not': 'not_equal',
        **KeyConditionExpression.FilterWordMap
    }

    def in_values(self, key: str, val: list, raw=False):
        k = self._register_key(key)
        s = []
        for v in val:
            s.append(self._register_value(v))
        sa = ', '.join(s)
        op = f'{k} IN ({sa})'
        self.operations += op

    def not_equal(self, key: str, val: dict, raw=False):
        self._make_comp('<>', key, val, raw)


class FilterConditionExpression(ConditionExpression):
    ParameterLabel = 'FilterExpression'
    ValuePrefix = 'filter'

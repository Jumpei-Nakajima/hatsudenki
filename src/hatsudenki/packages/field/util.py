from datetime import datetime
from uuid import UUID


def primal_deserializer(d):
    """
    基本型のデシリアライザ。DynamoDBからPythonに変換する。

    :param d: 対象値
    :return: Pythonで表現された値
    """

    k = list(d.keys())[0]
    v = d[k]
    if k == 'N':
        return int(v)
    if k in {'SS', 'BS'}:
        return {vv for vv in v}
    if k == 'NS':
        return {int(i) for i in v}
    if k == 'M':
        return {k: primal_deserializer(vv) for k, vv in v.items()}
    if k == 'L':
        return [primal_deserializer(vv) for vv in v]
    if k in {'B', 'BOOL'}:
        return v

    return d[k]


def _to_type_string(val):
    """
    型からDynamoDBシグネチャに変換する。

    :param val: 対象値
    :return: DynamoDBで有効な型シグネチャ
    """

    tp = type(val)
    if tp is int:
        return 'N'
    if tp is set:
        t = list(val)[0]
        return _to_type_string(t) + 'S'
    if tp is str:
        return 'S'
    if tp is bytes:
        return 'B'
    if tp is dict:
        return 'M'
    if tp is list:
        return 'L'
    if tp is bool:
        return 'BOOL'
    else:
        return '__extra__'


def primal_serializer(val):
    """
    基本型のシリアライザ。DynamoDBからPythonに変換する

    :param val: DynamoDBドキュメント
    :return: pythonでの値
    """
    s = _to_type_string(val)

    if s in 'L':
        return {s: [primal_serializer(ss) for ss in val]}
    if s in {'SS', 'NS', 'BS'}:
        return {s: [ss for ss in val]}
    if s == 'M':
        return {s: {k: primal_serializer(v) for k, v in val.items()}}
    if s in {'B', 'BOOL'}:
        return {s: val}
    if s == '__extra__':
        if type(val) is UUID:
            return primal_serializer(val.hex)
        if type(val) is datetime:
            return primal_serializer(val.timestamp())
        else:
            raise Exception(f'invalid type {val} {type(val)}')

    return {s: str(val)}

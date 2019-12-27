from collections.__init__ import defaultdict
from enum import Enum
from typing import DefaultDict, List

from hatsudenki.packages.expression.base import BaseExpression


class UpdateReturnType(Enum):
    # 何も返さない
    Nothing = 'NONE'
    # 古い（更新前の）アイテムを返す
    AllOld = 'ALL_OLD'
    # 影響を受けた属性の更新前の値を返す
    UpdatedOld = 'UPDATED_OLD'
    # 更新後のアイテムを返す
    AllNew = 'ALL_NEW'
    # 影響を受けた属性の更新後の値を返す
    UpdatedNew = 'UPDATED_NEW'


class UpdateOperation(Enum):
    # 値のセット
    Set = 'SET'
    # 属性の削除
    Remove = 'REMOVE'
    # 値の加減算（数値型及びセット型のみ）
    Add = 'ADD'
    # セットから値を削除（セット型のみ）
    Delete = 'DELETE'


class UpdateExpression(BaseExpression):
    """
    更新式クラス
    """

    ParameterLabel = 'UpdateExpression'

    def __init__(self):
        super().__init__()
        self.operations: DefaultDict[UpdateOperation, List[str]] = defaultdict(list)

    def set(self, key: str, val: any, raw=False):
        """
        SETオペレーションを追加
        :param key: 対象キー名
        :param val: 対象値
        :param raw: シリアライズされた値が渡されているか
        :return: None
        """
        v = self._register_value(val, raw)
        if v is None:
            return
        k = self._register_key(key)
        exp = f'{k} = {v}'

        self.operations[UpdateOperation.Set].append(exp)

    def increment(self, key: str, val: any, raw=False):
        """
        インクリメント
        内部で'SET #key = #key + :val'を発行するので実質SET
        :param key: 対象キー名
        :param val:
        :param raw:
        :return:
        """
        v = self._register_value(val, raw)
        if v is None:
            return
        k = self._register_key(key)
        exp = f'{k} = {k} + {v}'

        self.operations[UpdateOperation.Set].append(exp)

    def remove(self, *keys: str):
        """
        Removeオペレーションを追加
        :param key: 対象キー名
        :param val: 対象値
        :param raw: シリアライズされた値が渡されているか
        :return: None
        """
        ks = ', '.join(map(self._register_key, keys))
        self.operations[UpdateOperation.Remove].append(ks)

    def remove_raw_key(self, *keys: str):
        ks = ', '.join(keys)
        self.operations[UpdateOperation.Remove].append(ks)

    def add(self, key: str, val: any, raw=False):
        """
        ADDオペレーションを追加
        ADDは数値型及びセットデータ型しかサポートされません
        :param key: 対象キー名
        :param val: 対象値
        :param raw: シリアライズされた値が渡されているか
        :return: None
        """
        v = self._register_value(val, raw)
        if v is None:
            return
        k = self._register_key(key)
        exp = f'{k} {v}'
        self.operations[UpdateOperation.Add].append(exp)

    def delete(self, key: str, val: any, raw=False):
        """
        Deleteオペレーションを追加
        Deleteはデータセット型しかサポートされません
        :param key: 対象キー名
        :param val: 対象値
        :param raw: シリアライズされた値が渡されているか
        :return: None
        """
        if val is None:
            return

        k = self._register_key(key)
        v = self._register_value(val, raw)
        exp = f'{k} {v}'
        self.operations[UpdateOperation.Delete].append(exp)

    def list_append(self, key: str, val: any, raw=False):
        if val is None:
            return

        k = self._register_key(key)
        v = self._register_value(val, raw)

        exp = f'{k} = list_append({k}, {v})'
        self.operations[UpdateOperation.Set].append(exp)

    def list_concat(self, key: str, val: List[any], raw=False):
        if val is None:
            return

        k = self._register_key(key)
        v = self._register_value(val, raw)

        exp = f'list_append({k}, {v})'
        self.operations[UpdateOperation.Set].append(exp)

    @property
    def expression(self):
        """
        AWSにわたす形の表現文字列を取得
        :return: AWSにわたすことのできる表現式文字列
        """
        r = []
        for key, val in self.operations.items():
            r.append(f'{key.value} ' + ', '.join(val))
        return ' '.join(r)

    def dump(self):
        p = self.expression
        for k, v in self.names.items():
            p = p.replace(k, str(v))
        p = p.replace(',', '\n')

        for k, v in self.values.items():
            p = p.replace(k, str(v))

        print(p)

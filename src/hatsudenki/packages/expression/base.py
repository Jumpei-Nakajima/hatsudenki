from enum import Enum


class BaseExpression(object):
    ParameterLabel = ''
    ValuePrefix = ''

    class ParameterAttributeName(Enum):
        Names = 'ExpressionAttributeNames'
        Values = 'ExpressionAttributeValues'

    def __init__(self):
        self.label = self.__class__.ParameterLabel
        self.prefix = self.__class__.ValuePrefix
        self.names = {}
        self._rev_names = {}
        self.values = {}
        self._rev_vals = {}
        self.val_num = 0
        self.key_num = 0

    @staticmethod
    def _put_dict_scalar(d: dict, k: str, val: any):
        if val is None:
            return

        t = type(val)
        if t in [str, list] and len(val) is 0:
            return
        if t is dict and not val:
            return

        d[k] = val

    @staticmethod
    def _put_dict_dict(d: dict, k: str, val: dict):
        if not val:
            return
        d[k] = val

    @staticmethod
    def _merge_dict_dict(d: dict, k: str, val: dict):
        if val is None:
            return
        if not val:
            return
        n: dict = d.get(k, None)
        if n is not None:
            n.update(val)
        else:
            d[k] = val

    @property
    def expression(self):
        raise NotImplementedError()

    def to_parameter(self, *merge_items: 'BaseExpression'):
        ret = {}

        val_label = self.ParameterAttributeName.Names.value
        name_label = self.ParameterAttributeName.Values.value

        self._put_dict_scalar(ret, str(self.label), self.expression)
        self._put_dict_dict(ret, val_label, self.names)
        self._put_dict_dict(ret, name_label, self.values)

        for m in merge_items:
            if m is None:
                continue
            self._put_dict_scalar(ret, str(m.label), m.expression)
            self._merge_dict_dict(ret, val_label, m.names)
            self._merge_dict_dict(ret, name_label, m.values)

        return ret

    @classmethod
    def merge(cls, *items: 'BaseExpression'):
        i = len(items)
        if i is 0:
            return {}

        arg = [item for item in items if item is not None]
        if len(arg) is 1:
            return arg[0].to_parameter()
        return arg[0].to_parameter(*arg[1:])

    def _register_key(self, key: str):
        """
        キー名を追加
        :param key: 追加するキー名
        :return: プレースホルダー文字列
        """
        # TODO: DictMapの更新を行った際に同じ内容のキーが複数生成されてしまう。すでにあるやつは使い回すべき

        # .で分割しないとmap型の更新に対応できない
        sps = key.split('.')

        r = []
        for sp in sps:
            # 他のExpressionとかぶらないようにPrefixを付与しておく（一応
            k = f'#{self.prefix}_key__{self.key_num}'

            # 配列判定
            bracket_pos = sp.find('[')
            if bracket_pos >= 0:
                bracket_end = sp.index(']')
                idx = sp[bracket_pos + 1:bracket_end]
                sp = sp[0:bracket_pos]

                old = self._rev_names.get(sp)
                if old:
                    k = old
                else:
                    self.names[k] = sp
                    self._rev_names[sp] = k
                    self.key_num += 1
                r.append(f'{k}[{idx}]')
            else:
                old = self._rev_names.get(sp)
                if old:
                    k = old
                else:
                    self.names[k] = sp
                    self._rev_names[sp] = k
                    self.key_num += 1
                r.append(k)
        return '.'.join(r)

    def _register_value(self, value: any, raw=False):
        """
        値を追加
        :param value: 追加する値
        :param raw: すでにシリアライズされている値を直接代入する場合はTrue
        :return: プレースホルダー文字列
        """
        # 循環参照の回避
        # rawフラグが設定されていない場合は内部でprimalシリアライザを使用する
        from hatsudenki.packages.field import primal_serializer
        sv = value if raw else primal_serializer(value)
        if sv is None:
            # DynamoDBは値をNoneで保持できないのでスキップ
            # （Noneを返すとクライアント側がスキップするようになっている）
            return None

        # TODO: 値の方のクエリ圧縮はそもそも必要なのか。tupleだとListで死ぬのでjsonで判定している（ダサい）
        # hashed_val = ujson.dumps(sv)
        # already = self._rev_vals.get(hashed_val)
        # if already:
        #     k = already
        # else:
        #     k = f':{self.prefix}_value__{self.val_num}'
        #     self.values[k] = sv
        #     self._rev_vals[hashed_val] = k
        #     self.val_num += 1

        k = f':{self.prefix}_value__{self.val_num}'
        self.values[k] = sv
        self.val_num += 1

        return k

    def is_empty(self):
        return False

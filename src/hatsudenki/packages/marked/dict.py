from typing import Generic, TypeVar, Type, Optional, Dict, Iterator

from hatsudenki.packages.expression.update import UpdateExpression
from hatsudenki.packages.marked import MarkedObject, Markable

T = TypeVar('T')


class MarkedObjectWithIndex(MarkedObject):
    """
    Hashキーを指定できるMarkedObject
    """

    class Meta:
        # ハッシュキーの名前
        hash_key = ''

    @classmethod
    def get_hash_key_name(cls):
        """
        ハッシュキーの名前を取得

        :return: str
        """
        return cls.Meta.hash_key

    def get_hash_value(self):
        """
        ハッシュキーの値を取得

        :return: str
        """
        return str(getattr(self, self.__class__.get_hash_key_name()))

    def __init__(self, name, parent: Markable, **kwargs):
        super().__init__(name, parent, **kwargs)
        self._is_appended = True

    def flush(self):
        super().flush()
        # 値が空のときにappendフラグを追ってしまうと新規追加を検知できない
        # そのままupdateされてもmarkが空なのでどのみちクエリは発行されないのでOK
        if not self.__class__.is_empty(self):
            self._is_appended = False

    def build_update_expression(self, parent_name, upd: UpdateExpression):
        # 新規追加の際は無条件に全部
        if self._is_appended:
            upd.set(parent_name, self.serialized_value, raw=True)
        else:
            super().build_update_expression(parent_name, upd)


class MarkedObjectDict(Generic[T]):
    def __init__(self, name: str, target_class: Type[T], parent: Optional[Markable], item: Optional[dict] = None):
        """
        イニシャライザ

        :param name: フィールド名前
        :param target_class: 子として抱えるクラスタイプ
        :param parent: 親Markable
        :param item: 要素
        """
        self.name = name
        self._target_class = target_class
        self._mark = set()
        self._item: Dict[any, T] = {}
        if item:
            for k, v in item.items():
                self._item[k] = target_class(k, self, **v)
        self._parent = parent
        self._is_append = True

    def __iter__(self) -> Iterator[T]:
        """
        イテレータ

        :return: iterator
        """
        return self._item.__iter__()

    def items(self):
        """
        dict.items()

        :return: k,v
        """
        return self._item.items()

    def values(self):
        """
        dict.values()

        :return: v
        """
        return self._item.values()

    def keys(self):
        """
        dict.keys()

        :return: k
        """
        return self._item.keys()

    def modify_mark(self, key: str):
        """
        変更されたことをマーク

        :param key: 対象キー名
        :return:
        """
        self._mark.add(key)
        if self._parent:
            self._parent.modify_mark(self.name)

    def flush(self):
        """
        マークをクリア

        :return:
        """
        self._mark.clear()

        # 値が空のときにappendフラグを追ってしまうと新規追加を検知できない
        # そのままupdateされてもmarkが空なのでどのみちクエリは発行されないのでOK
        if not self.is_empty():
            self._is_append = False

    def set_dict(self, d: Dict[str, 'MarkedObjectDict[T]']):
        """
        Dictionaryを直接セットする。このメソッドは更新をマークしない

        :param d: セットするDict
        :return:
        """
        self._item = d

    @classmethod
    def serialize(cls, data: 'MarkedObjectDict[T]'):
        """
        Python → DynamoDB

        :param data: 対象データ
        :return: dict
        """
        ret = {}
        for k, v in data._item.items():
            if not v:
                continue
            ret[k] = v.serialized_value
        return {'M': ret}

    @property
    def serialized_value(self):
        """
        シリアライズされた値

        :return: dict
        """
        return self.__class__.serialize(self)

    @classmethod
    def deserialize(cls, name, target_class: Type[T], data: dict, parent: Markable):
        """
        DynamoDB → Python

        :param name: フィールド名前
        :param target_class: 抱える子タイプ
        :param data: DynamoDBドキュメント
        :param parent: 親Markable
        :return: 生成された値
        """
        ret = cls(name, target_class, parent)
        ret.set_dict({k: target_class.deserialize(k, v, ret) for k, v in data['M'].items()})
        return ret

    def is_empty(self):
        """
        値が空か

        :return: boolean
        """
        for k, v in self._item.items():
            if v and not self._target_class.is_empty(v):
                return False
        return True

    def append(self, value: MarkedObjectWithIndex):
        """
        要素を追加。すでに同一キーの要素が存在した場合は上書きする

        :param value: 追加する値
        :return: None
        """
        self.__setitem__(value.get_hash_value(), value)

    def __getitem__(self, item) -> T:
        """
        値を[xxx]で取得。存在しない場合は例外が発生する。

        :param item: 取得するキー名
        :return: 指定されたキーに対応する要素。
        """
        item_str = str(item)
        return self._item[item_str]

    def __setitem__(self, key, value):
        """
        値を[xxx]で設定。基本的にはappendメソッドを使用すること。

        :param key: セットするキー名
        :param value: セットする値
        :return: None
        """
        key_str = str(key)
        assert (isinstance(value, self._target_class))
        assert (value.get_hash_value() == key_str)
        self._item[key_str] = value
        self.modify_mark(key_str)

    # TODO: 仮対応
    def clear(self):
        self._item: Dict[any, T] = {}
        self._is_append = True

    def create(self, **kwargs) -> T:
        """
        対応する子クラスをインスタンス化して返却する

        :param kwargs: 子クラスのイニシャライザにわたす値
        :return: インスタンス化された子クラス
        """
        return self._target_class(name=None, parent=None, **kwargs)

    def build_update_expression(self, upd: UpdateExpression):
        """
        差分を考慮したupdate条件オブジェクトをセット

        :param upd: セット対象のUpdateExpressionインスタンス
        :return: None
        """

        # 追加されたものは無条件に全て
        if self._is_append:
            upd.set(self.name, self.serialized_value, raw=True)
            return

        if len(self._mark) is 0:
            # 更新されたものは無い
            return

        for update_key in self._mark:
            now_value = self[update_key]
            fc = self._target_class

            n = f'{self.name}.{update_key}'
            if fc.is_empty(now_value):
                # 値が空になった
                upd.remove(n)
                continue
            now_value.build_update_expression(n, upd)

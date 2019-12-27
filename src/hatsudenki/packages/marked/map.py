from typing import Generic, Type, Union, TypeVar

from hatsudenki.packages.expression.update import UpdateExpression
from hatsudenki.packages.marked import Markable

T = TypeVar('T')


class MarkedObjectMap(Generic[T]):
    def __init__(self, name: str, target_class: Type[T], parent: Markable, item: Union[T, dict, None] = None):
        """
        イニシャライザ

        :param name: 名前
        :param target_class: 対象クラス名
        :param parent: 親オブジェクト
        :param item: 初期化パラメータ
        """
        self.name = name
        self._target_class: Type[T] = target_class
        self._mark = set()
        self._is_assign = False
        self._parent = parent
        self._item: T = None
        if isinstance(item, target_class):
            # 値が直接渡された
            self._item = item
        elif type(item) is dict:
            # dict
            self._item = target_class(name, self, **item)

    def assign(self, value: T):
        """
        値をアサインする

        :param value: アサインする値
        :return: None
        """
        self._item = value
        # 名前をセットしておく
        value._name = self.name
        # マーク
        self.assign_mark()

    @property
    def value(self) -> T:
        """
        アサインされている値

        :return: T
        """

        return self._item

    def assign_mark(self):
        """
        値をアサインされたことをマークする

        :return: None
        """
        self._is_assign = True
        if self._parent:
            self._parent.modify_mark(self.name)

    def modify_mark(self, key):
        """
        値が更新されたことをマークする
        :param key:
        :return:
        """
        self._mark.add(key)
        if self._parent:
            self._parent.modify_mark(self.name)

    def flush(self):
        """
        変更情報をクリア

        :return: None
        """
        self._mark.clear()
        self._is_assign = False

    @classmethod
    def serialize(cls, data: 'MarkedObjectMap[T]'):
        """
        DynamoDBへシリアライズ

        :param data: 対象データ
        :return: DynamoDB dict
        """

        return data._item.serialized_value

    @property
    def serialized_value(self):
        """
        DynamoDB dictにシリアライズされた値

        :return: DynamoDB dict
        """
        return self.__class__.serialize(self)

    @classmethod
    def deserialize(cls, name, target_class: Type[T], data: dict, parent: Markable):
        """
        pythonにデシリアライズ
        :param name: 名前
        :param target_class: 格納対象クラスタイプ
        :param data: dynamoDB dict
        :param parent: 親オブジェクト
        :return: 自分自身をインスタンス化したものy
        """
        ret = cls(name, target_class, parent)
        ret._item = target_class.deserialize(name, data, ret)
        return ret

    def build_update_expression(self, upd: UpdateExpression):
        """
        更新箇所を考慮したクエリをビルド。
        追加と更新が同時に行われた場合は例外が発生する

        :param upd: UpdateExpressionインスタンス
        :return: None
        """
        if self._item is None:
            return

        if self._is_assign:
            upd.set(self.name, self.serialized_value, raw=True)
            return

        # 更新キーを考慮して小要素を直接セットするクエリをビルド
        self._item.build_update_expression(self.name, upd)

    def create(self, **kwargs) -> T:
        """
        格納可能なインスタンスを生成して返す（追加はしない）

        :param kwargs: 初期値
        :return: 生成されたインスタンス
        """
        return self._target_class(name=None, parent=None, **kwargs)

    def is_empty(self):
        return self._target_class.is_empty(self._item)

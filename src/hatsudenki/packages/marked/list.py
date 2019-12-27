from typing import Generic, Type, Union, List, Iterator, TypeVar

from hatsudenki.packages.expression.update import UpdateExpression
from hatsudenki.packages.marked import Markable

T = TypeVar('T')


class MarkedObjectList(Generic[T]):
    """
    MarkedObjectのリスト
    """

    def __init__(self, name: str, target_class: Type[T], parent: Markable,
                 items: Union[List[T], 'MarkedObjectList[T]', None] = None):
        """
        イニシャライザ

        :param name: 名前
        :param target_class: 格納可能なインスタンスタイプ
        :param parent: 親オブジェクト
        :param items: 初期値
        """
        if isinstance(items, MarkedObjectList):
            self._list: List[T] = items._list
        elif type(items) is list:
            self._list: List[T] = items
        else:
            self._list: List[T] = []
        self.name = name
        self._target_class: Type[T] = target_class
        self._mark = set()
        self._appended = []
        self._parent = parent
        self._mark_replace = False

    def __len__(self):
        return len(self._list)

    def append(self, data: T):
        """
        末尾に追加

        :param data: 追加するデータ
        :return: None
        """
        data._parent = self
        i = len(self._list)
        data.name = i
        # data.set_appended()
        self._list.append(data)
        self.append_mark(data)

    def replace(self, data: List[T]):
        """
        置き換え

        :param data:
        :return:
        """
        self.flush()
        self.set_list(data)
        self.replace_mark()

    def __getitem__(self, item: int) -> T:
        """
        [x]形式でアクセス

        :param item: 添字
        :return: T
        """
        return self._list[item]

    def __setitem__(self, key: int, value: T):
        """
        [x]形式でセット。変更もマークする

        :param key: 添字
        :param value: セットする値
        :return: None
        """
        if value is not None:
            # 親情報をセット
            value._parent = self._parent
            value.name = key

        self._list[key] = value
        # 更新をマーク
        self.modify_mark(key)

    def __iter__(self) -> Iterator[T]:
        """
        イテレータ

        :return: iterator
        """
        return self._list.__iter__()

    def create(self, **kwargs) -> T:
        """
        格納可能なインスタンスを生成して返す（追加はしない）

        :param kwargs: 初期値
        :return: 生成されたインスタンス
        """
        return self._target_class(name=None, parent=None, **kwargs)

    def modify_mark(self, key):
        """
        更新をマーク
        :param key: 更新されたキー
        :return: None
        """

        if self._parent:
            # 親が設定されていた場合は親にも伝搬する
            self._parent.modify_mark(self.name)
        self._mark.add(key)

    def append_mark(self, data: T):
        """
        要素追加をマーク

        :param data: 追加された要素
        :return:
        """
        if self._parent:
            # 親が設定されていた場合は親に更新を伝搬する
            self._parent.modify_mark(self.name)
        # 追加された要素リストに追加
        self._appended.append(data)

    def replace_mark(self):
        """
        置き換えをマーク
        ※重いよ

        :return: None
        """
        if self._parent:
            # 親が設定されていた場合は親に更新を伝搬する
            self._parent.modify_mark(self.name)
        self._mark_replace = True

    def flush(self):
        """
        変更情報をクリア

        :return: None
        """
        self._mark.clear()
        self._appended.clear()
        self._mark_replace = False

    def set_list(self, d: List[T]):
        """
        リストを直接セットする。この処理は更新をマークしない。

        :param d: セットするリスト
        :return:
        """
        self._list = d

    @classmethod
    def serialize(cls, data: 'MarkedObjectList[MarkedObject]'):
        """
        DynamoDBへシリアライズ

        :param data: 対象データ
        :return: DynamoDB dict
        """
        ret = [d.serialized_value for d in data]
        return {'L': ret}

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
        der = target_class.deserialize
        ret = cls(name, target_class, parent)
        ret.set_list([der(idx, i, ret) for idx, i in enumerate(data['L'])])
        return ret

    def build_update_expression(self, upd: UpdateExpression):
        """
        更新箇所を考慮したクエリをビルド。
        追加と更新が同時に行われた場合は例外が発生する

        :param upd: UpdateExpressionインスタンス
        :return: None
        """

        if self._mark_replace:
            # replaceがマークされているときはまるっと上書き。Replaceマークはすべてのマークより優先される
            upd.set(
                self.name,
                {'L': [u.serialized_value for u in self._list]},
                raw=True
            )
            return

        # 一つの要素に対し二度オペレーションを行うことはできない(DynamoDBの制限)
        # if len(self._appended) > 0 and len(self._mark) > 0:
        #     raise Exception('追加と更新を同時に行うことはできません')

        # 追加された要素がある場合はlist_appendを発行する
        if len(self._appended) > 0:
            upd.list_append(self.name, {'L': [u.serialized_value for u in self._appended]}, raw=True)
            # return

        # 更新キーを考慮して小要素を直接セットするクエリをビルド
        for update_key in self._mark:
            now_value = self[update_key]
            fc = self._target_class

            n = f'{self.name}[{update_key}]'
            if fc.is_empty(now_value):
                # 空になった場合はremoveを発行する
                upd.remove_raw_key(n)
                continue

            now_value.build_update_expression(n, upd)

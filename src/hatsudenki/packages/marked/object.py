from typing import Dict

from hatsudenki.packages.expression.update import UpdateExpression
from hatsudenki.packages.marked import SerializableField, Markable


class MarkedObject(object):
    """
    更新されたアトリビュートを保持し、更新箇所だけ変更するクエリをビルドできるオブジェクト
    """
    # フィールドクラスを文字列で引けるようにするための参照キャッシュ
    __fields: Dict[str, SerializableField] = {}

    class Field:
        """
        継承先ではここにフィールドを定義する
        """
        pass

    def __init_subclass__(cls, **kwargs):
        # フィールドを文字列で参照できるようにキャッシュしておく
        cls.__fields = {}
        for k in dir(cls.Field):
            if k.startswith('__'):
                # システム予約の属性なのでフィールドではない
                continue
            # 取り出して連想配列に格納
            fc = getattr(cls.Field, k)
            from hatsudenki.packages.field import BaseHatsudenkiField
            if isinstance(fc, BaseHatsudenkiField):
                cls.__fields[k] = fc
                # ついでにフィールドに自分自身の名前を教えておく
                fc.name = k

    def __init__(self, name, parent: Markable, **kwargs):
        """
        イニシャライザ

        :param name: 名前
        :param parent: 親オブジェクト
        :param kwargs: メンバ初期化用
        """
        self._mark = set()
        self.name = name
        self._parent = parent

    def __setattr__(self, key, value):
        # 最初の設定ではない、且つフィールドを編集しようとしている
        if hasattr(self, key) and key in self.__class__.__fields:
            self.modify_mark(key)
        super().__setattr__(key, value)

    def modify_mark(self, key):
        """
        変更をマーク

        :param key: 変更したキー
        :return: None
        """
        self._mark.add(key)
        # 親オブジェクトが設定されていればマークを伝搬する
        if self._parent:
            self._parent.modify_mark(self.name)

    def flush(self):
        """
        変更情報をクリア

        :return: None
        """
        self._mark.clear()

    @classmethod
    def get_field_class(cls, name: str) -> SerializableField:
        """
        フィールド名から対応するフィールドクラスを取得

        :param name: フィールド名
        :return: フィールドクラス
        """
        return getattr(cls.Field, name)

    @classmethod
    def deserialize(cls, name, data: dict, parent: Markable):
        """
        DynamoDB → Python

        :param name: 名前
        :param data: dynamoDB dict
        :param parent: 親オブジェクト
        :return: 自分自身をインスタンス化したもの
        """
        a = cls(name=name, parent=parent)
        m = data['M']
        d = {}
        for k, f in cls.__fields.items():
            a.__setattr__(k, f.deserialize(m.get(k, None), a))
        return a

    @classmethod
    def serialize(cls, data: 'MarkedObject'):
        """
        Python → DynamoDB

        :param data: 変換対象
        :return: dynamoDB dict
        """
        ret = {
            'M': {k: v.serialize(getattr(data, k)) for k, v in data.__fields.items() if
                  not v.is_empty(getattr(data, k))}
        }
        return ret

    @property
    def serialized_value(self):
        """
        DynamoDB dictにシリアライズされた値

        :return: dynamoDB dict
        """
        return self.__class__.serialize(self)

    @classmethod
    def is_empty(cls, data):
        """
        値が空か判定

        :param data: 判定対象
        :return: boolean
        """
        return data is None

    def build_update_expression(self, parent_name, upd: UpdateExpression):
        """
        変更点を抽出してUpdateExpressionに追加する

        :param parent_name: 親の属性名
        :param upd: UpdateExpressionインスタンス
        :return: None
        """
        for update_key in self._mark:
            now_value = getattr(self, update_key)
            fc = self.__class__.get_field_class(update_key)
            n = parent_name + '.' + str(update_key)
            upd.set(n, fc.serialize(now_value), raw=True)

from typing import Type, TypeVar, Iterator, Iterable

from hatsudenki.packages.table.index import PrimaryIndex

T = TypeVar('T')


class CacheBaseTableSolo(object):
    class Meta:
        table_name: str = None
        primary_index: PrimaryIndex = None

    _mapped = {}

    def __init__(self, **kwargs):
        pass

    @classmethod
    def _get(cls: Type[T], hash_key, range_key=None) -> T:
        """
        HASHキーを指定して一件取得

        :param hash_key: ハッシュキー値
        :param range_key: レンジキー値（Soloテーブルはこの値は必ずNoneになる）
        :return: 条件にマッチしたレコード
        """
        if range_key is not None:
            raise Exception('invalid key.')

        return cls._mapped[hash_key]

    @classmethod
    def _get_by_cursor(cls: Type[T], cursor: str) -> T:
        """
        カーソル文字列から一件取得

        :param cursor: カーソル文字列
        :return: 条件にマッチしたレコード
        """

        return cls._get(cursor)

    @classmethod
    def _check_exist_hash(cls, hash_key):
        """
        指定されたハッシュキーのレコードがあるか判定する

        :param hash_key: 検索対象値
        :return: bool
        """
        return hash_key in cls._mapped

    @classmethod
    def _iter(cls: Type[T]) -> Iterator[T]:
        """
        全件列挙イテレータ

        :return: iter
        """
        for d in cls._mapped.values():
            yield d

    @classmethod
    def get_hash_key_name(cls):
        """
        HASHキーの名前を取得する

        :return: ハッシュキー文字列
        """
        return cls.Meta.primary_index.hash_key

    @property
    def hash_value(self):
        """
        ハッシュキーの値を取得

        :return: ハッシュキーに設定された値
        """
        return getattr(self, self.get_hash_key_name())

    @classmethod
    def _set_records(cls, recs: Iterable[dict]):
        """
        レコードをセット

        :param recs: セットするレコード配列
        :return: None
        """

        hk = cls.get_hash_key_name()
        cls._mapped = {}
        for rec in recs:
            cls._mapped[rec[hk]] = cls(**rec)

    @property
    def one_cursor(self):
        """
        レコード一件を一意に表す文字列

        :return: str
        """
        return str(self.hash_value)

    def __str__(self) -> str:
        return self.one_cursor

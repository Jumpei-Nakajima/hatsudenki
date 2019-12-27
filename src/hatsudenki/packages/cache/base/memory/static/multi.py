from typing import Type, Iterator, List

from hatsudenki.packages.cache.base.memory.multi import CacheBaseTableMulti
from hatsudenki.packages.cache.base.memory.solo import T
from hatsudenki.packages.cache.base.memory.static.solo import StaticCacheBaseTableSolo


class StaticCacheBaseTableMulti(CacheBaseTableMulti, StaticCacheBaseTableSolo):
    @classmethod
    def get(cls: Type[T], hash_key, range_key=None) -> T:
        return cls._get(hash_key, range_key)

    @classmethod
    def find(cls: Type[T], hash_key) -> List[T]:
        """
        HASHキーのみを指定してリスト
        :param hash_key: ハッシュキー
        :return: レコードの配列
        """

        return cls._find(hash_key)

    @classmethod
    def find_left(cls: Type[T], hash_key, range_key=None) -> T:
        return cls._find_left(hash_key, range_key)

    @classmethod
    def find_right(cls: Type[T], hash_key, range_key=None) -> T:
        return cls._find_right(hash_key, range_key)

    @classmethod
    def find_lte(cls: Type[T], hash_key: str, range_key) -> List[T]:
        """
        RANGEキー条件指定検索 - 以下

        :param hash_key: ハッシュキー
        :param range_key: 対象値
        :return: レコードの配列
        """

        return cls._find_lte(hash_key, range_key)

    @classmethod
    def find_lt(cls: Type[T], hash_key: str, range_key) -> List[T]:
        """
        RANGEキー条件指定検索 - 未満

        :param hash_key: ハッシュキー
        :param range_key: 対象値
        :return: レコードの配列
        """

        return cls._find_lt(hash_key, range_key)

    @classmethod
    def find_gte(cls: Type[T], hash_key: str, range_key) -> List[T]:
        """
        RANGEキー条件指定検索 - 以上

        :param hash_key: ハッシュキー
        :param range_key: 対象値
        :return: レコードの配列
        """

        return cls._find_gte(hash_key, range_key)

    @classmethod
    def find_gt(cls: Type[T], hash_key: str, range_key) -> List[T]:
        """
        RANGEキー条件指定検索 - より小さい

        :param hash_key: ハッシュキー
        :param range_key: 対象値
        :return: レコードの配列
        """

        return cls._find_gt(hash_key, range_key)

    @classmethod
    def find_between(cls: Type[T], hash_key: str, left_range, right_range) -> List[T]:
        """
        RANGEキー条件指定検索 - 範囲

        :param hash_key: ハッシュキー
        :param left_range: 左辺値
        :param right_range: 右辺値
        :return: レコードの配列
        """

        return cls._find_between(hash_key, left_range, right_range)

    @classmethod
    def get_by_cursor(cls: Type[T], cursor: str) -> T:
        return cls._get_by_cursor(cursor)

    @classmethod
    def find_by_cursor(cls: Type[T], cursor: str) -> List[T]:
        """
        集合カーソルを指定してリストを取得

        :param cursor: str
        :return: レコードの配列
        """

        return cls._find_by_cursor(cursor)

    @classmethod
    def iter(cls: Type[T]) -> Iterator[T]:
        return cls._iter()

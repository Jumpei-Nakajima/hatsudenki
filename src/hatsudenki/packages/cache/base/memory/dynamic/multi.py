from typing import Type, Iterator, List

from hatsudenki.packages.cache.base.memory.dynamic.solo import DynamicCacheBaseTableSolo
from hatsudenki.packages.cache.base.memory.multi import CacheBaseTableMulti
from hatsudenki.packages.cache.base.memory.solo import T


class DynamicCacheBaseTableMulti(CacheBaseTableMulti, DynamicCacheBaseTableSolo):
    _mapped = None
    _sorted_table = None

    @classmethod
    def invalidate(cls):
        super().invalidate()
        cls._sorted_table = None

    @classmethod
    def prepare(cls):
        super().prepare()
        cls._sorted_table = {}

    @classmethod
    async def get(cls: Type[T], hash_key, range_key=None) -> T:
        await cls._setup()
        return cls._get(hash_key, range_key)

    @classmethod
    async def find(cls: Type[T], hash_key) -> List[T]:
        """
        HASHキーのみを指定してリスト
        :param hash_key: ハッシュキー
        :return: レコードの配列
        """
        await cls._setup()
        return cls._find(hash_key)

    @classmethod
    async def find_left(cls: Type[T], hash_key, range_key=None) -> T:
        await cls._setup()
        return cls._find_left(hash_key, range_key)

    @classmethod
    async def find_right(cls: Type[T], hash_key, range_key=None) -> T:
        await cls._setup()
        return cls._find_right(hash_key, range_key)

    @classmethod
    async def find_lte(cls: Type[T], hash_key: str, range_key) -> List[T]:
        """
        RANGEキー条件指定検索 - 以下

        :param hash_key: ハッシュキー
        :param range_key: 対象値
        :return: レコードの配列
        """
        await cls._setup()
        return cls._find_lte(hash_key, range_key)

    @classmethod
    async def find_lt(cls: Type[T], hash_key: str, range_key) -> List[T]:
        """
        RANGEキー条件指定検索 - 未満

        :param hash_key: ハッシュキー
        :param range_key: 対象値
        :return: レコードの配列
        """
        await cls._setup()
        return cls._find_lt(hash_key, range_key)

    @classmethod
    async def find_gte(cls: Type[T], hash_key: str, range_key) -> List[T]:
        """
        RANGEキー条件指定検索 - 以上

        :param hash_key: ハッシュキー
        :param range_key: 対象値
        :return: レコードの配列
        """
        await cls._setup()
        return cls._find_gte(hash_key, range_key)

    @classmethod
    async def find_gt(cls: Type[T], hash_key: str, range_key) -> List[T]:
        """
        RANGEキー条件指定検索 - より小さい

        :param hash_key: ハッシュキー
        :param range_key: 対象値
        :return: レコードの配列
        """
        await cls._setup()
        return cls._find_gt(hash_key, range_key)

    @classmethod
    async def find_between(cls: Type[T], hash_key: str, left_range, right_range) -> List[T]:
        """
        RANGEキー条件指定検索 - 範囲

        :param hash_key: ハッシュキー
        :param left_range: 左辺値
        :param right_range: 右辺値
        :return: レコードの配列
        """
        await cls._setup()
        return cls._find_between(hash_key, left_range, right_range)

    @classmethod
    async def get_by_cursor(cls: Type[T], cursor: str) -> T:
        await cls._setup()
        return cls._get_by_cursor(cursor)

    @classmethod
    async def find_by_cursor(cls: Type[T], cursor: str) -> List[T]:
        """
        集合カーソルを指定してリストを取得

        :param cursor: str
        :return: レコードの配列
        """
        await cls._setup()
        return cls._find_by_cursor(cursor)

    @classmethod
    async def iter(cls: Type[T]) -> Iterator[T]:
        await cls._setup()
        return cls._iter()

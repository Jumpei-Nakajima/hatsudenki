from logging import getLogger
from typing import Type, TypeVar, Iterator, List

from hatsudenki.packages.cache.base.memory.solo import CacheBaseTableSolo

T = TypeVar('T')

_logger = getLogger(__name__)


class DynamicCacheBaseTableSolo(CacheBaseTableSolo):
    _mapped = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    async def build(cls: Type[T]) -> List[T]:
        return []

    @classmethod
    async def _setup(cls):
        if cls.check_invalidate():
            _logger.info(f'setup {cls.Meta.table_name}')
            cls.prepare()
            r = await cls.build()
            cls._set_records(r)

    @classmethod
    def check_invalidate(cls):
        return cls._mapped is None

    @classmethod
    def invalidate(cls):
        cls._mapped = None

    @classmethod
    def prepare(cls):
        cls._mapped = {}

    @classmethod
    async def get(cls: Type[T], hash_key, range_key=None) -> T:
        """
        HASHキーを指定して一件取得

        :param hash_key: ハッシュキー値
        :param range_key: レンジキー値（Soloテーブルはこの値は必ずNoneになる）
        :return: 条件にマッチしたレコード
        """

        await cls._setup()
        return cls._get(hash_key, range_key)

    @classmethod
    async def get_by_cursor(cls: Type[T], cursor: str) -> T:
        """
        カーソル文字列から一件取得

        :param cursor: カーソル文字列
        :return: 条件にマッチしたレコード
        """
        await cls._setup()
        return cls._get_by_cursor(cursor)

    @classmethod
    async def check_exist_hash(cls, hash_key):
        """
        指定されたハッシュキーのレコードがあるか判定する

        :param hash_key: 検索対象値
        :return: bool
        """
        await cls._setup()
        return cls._check_exist_hash(hash_key)

    @classmethod
    async def iter(cls: Type[T]) -> Iterator[T]:
        """
        全件列挙イテレータ

        :return: iter
        """

        await cls._setup()
        return cls._iter()

import bisect
from typing import Type, Iterator, List, Iterable

from hatsudenki.packages.cache.base.define import CURSOR_SEPARATOR
from hatsudenki.packages.cache.base.memory.solo import CacheBaseTableSolo, T


class CacheBaseTableMulti(CacheBaseTableSolo):
    _sorted_table = {}

    @classmethod
    def _get(cls: Type[T], hash_key, range_key=None) -> T:
        if range_key is None:
            raise Exception('invalid key.')
        l = cls._sorted_table[hash_key]
        l_idx = bisect.bisect_left(l, range_key)
        if l_idx != len(l) and l[l_idx] == range_key:
            return cls._mapped[hash_key][l_idx]
        raise Exception(f'data not found. {cls.Meta.table_name} {hash_key} {range_key}')

    @classmethod
    def _find_left(cls: Type[T], hash_key, range_key=None) -> T:
        """
        たとえば[0,100,200,300]のリストに対して下記のように探索する
        50: 0
        100: 1
        150: 1
        200: 2

        :param hash_key:
        :param range_key:
        :return:
        """
        if range_key is None:
            raise Exception('invalid key.')
        l = cls._sorted_table[hash_key]
        l_idx = bisect.bisect_left(l, range_key)
        list_length = len(l)
        if l_idx >= list_length:
            l_idx = list_length - 1
        if range_key < l[l_idx]:
            l_idx -= 1
        if l_idx < 0:
            return cls._mapped[hash_key][0]
            # raise Exception(f'data not found. {cls.Meta.table_name} {hash_key} {range_key} {l_idx}')
        return cls._mapped[hash_key][l_idx]

    @classmethod
    def _find_right(cls: Type[T], hash_key, range_key=None) -> T:
        """
        たとえば[100,200,300]のリストに対して下記のように探索する
        50: 0
        100: 1
        150: 1
        200: 2

        :param hash_key:
        :param range_key:
        :return:
        """
        if range_key is None:
            raise Exception('invalid key.')
        l = cls._sorted_table[hash_key]
        r_idx = bisect.bisect_right(l, range_key)
        list_length = len(l)
        if r_idx >= list_length:
            r_idx = list_length - 1
        return cls._mapped[hash_key][r_idx]

    @classmethod
    def _find(cls: Type[T], hash_key) -> List[T]:
        """
        HASHキーのみを指定してリスト
        :param hash_key: ハッシュキー
        :return: レコードの配列
        """
        l = cls._mapped[hash_key]
        return l

    @classmethod
    def _find_lte(cls: Type[T], hash_key: str, range_key) -> List[T]:
        """
        RANGEキー条件指定検索 - 以下

        :param hash_key: ハッシュキー
        :param range_key: 対象値
        :return: レコードの配列
        """
        f = cls._sorted_table[hash_key]
        i = bisect.bisect_right(f, range_key)
        return cls._mapped[hash_key][0:i]

    @classmethod
    def _find_lt(cls: Type[T], hash_key: str, range_key) -> List[T]:
        """
        RANGEキー条件指定検索 - 未満

        :param hash_key: ハッシュキー
        :param range_key: 対象値
        :return: レコードの配列
        """
        f = cls._sorted_table[hash_key]
        i = bisect.bisect_left(f, range_key)
        return cls._mapped[hash_key][0:i]

    @classmethod
    def _find_gte(cls: Type[T], hash_key: str, range_key) -> List[T]:
        """
        RANGEキー条件指定検索 - 以上

        :param hash_key: ハッシュキー
        :param range_key: 対象値
        :return: レコードの配列
        """
        f = cls._sorted_table[hash_key]
        i = bisect.bisect_left(f, range_key)
        return cls._mapped[hash_key][i:]

    @classmethod
    def _find_gt(cls: Type[T], hash_key: str, range_key) -> List[T]:
        """
        RANGEキー条件指定検索 - より小さい

        :param hash_key: ハッシュキー
        :param range_key: 対象値
        :return: レコードの配列
        """
        f = cls._sorted_table[hash_key]
        i = bisect.bisect_right(f, range_key)
        return cls._mapped[hash_key][i:]

    @classmethod
    def _find_between(cls: Type[T], hash_key: str, left_range, right_range) -> List[T]:
        """
        RANGEキー条件指定検索 - 範囲

        :param hash_key: ハッシュキー
        :param left_range: 左辺値
        :param right_range: 右辺値
        :return: レコードの配列
        """
        f = cls._sorted_table[hash_key]
        l = bisect.bisect_left(f, left_range)
        r = bisect.bisect_right(f, right_range)
        return cls._mapped[hash_key][l:r]

    @classmethod
    def _get_by_cursor(cls: Type[T], cursor: str) -> T:
        sp = cursor.split(CURSOR_SEPARATOR, 2)
        return cls._get(*sp)

    @classmethod
    def _find_by_cursor(cls: Type[T], cursor: str) -> List[T]:
        """
        集合カーソルを指定してリストを取得

        :param cursor: str
        :return: レコードの配列
        """
        return cls._find(cursor)

    @classmethod
    def _iter(cls: Type[T]) -> Iterator[T]:
        for _, l in cls._mapped.items():
            for d in l:
                yield d

    @classmethod
    def get_range_key_name(cls):
        """
        RANGEキーの名前を取得する

        :return: str
        """
        return cls.Meta.primary_index.range_key

    @property
    def one_cursor(self):
        return str(self.hash_value) + CURSOR_SEPARATOR + str(self.range_value)

    @property
    def many_cursor(self):
        """
        集合を表す文字列

        :return: str
        """
        return str(self.hash_value)

    @property
    def range_value(self):
        """
        RANGEキーの値を取得

        :return: RANGEキーに設定されている値
        """
        return getattr(self, self.__class__.get_range_key_name())

    @classmethod
    def _set_records(cls, recs: Iterable[dict]):
        hk = cls.get_hash_key_name()
        rk = cls.get_range_key_name()
        d: dict = {}
        s: dict = {}

        for rec in recs:
            hk_val = rec[hk]
            rk_val = rec[rk]
            n = d.get(hk_val, [])
            idx = bisect.bisect_right(n, rk_val)
            n.insert(idx, rk_val)
            d[hk_val] = n

            ss = s.get(hk_val, [])
            ss.insert(idx, cls(**rec))
            s[hk_val] = ss

        cls._mapped = s
        cls._sorted_table = d

    @classmethod
    def _append(cls: Type[T], rec: T):
        hk = cls.get_hash_key()
        rk = cls.get_range_key()
        hk_val = rec[hk]
        rk_val = rec[rk]
        n = cls._sorted_table.get(hk_val, [])
        idx = bisect.bisect_left(n, rk_val)

        n.insert(idx, rk_val)
        cls._sorted_table[hk_val] = n

        ss = cls._mapped.get(hk_val, [])
        ss.insert(idx, cls(**rec))
        cls._mapped[hk_val] = ss

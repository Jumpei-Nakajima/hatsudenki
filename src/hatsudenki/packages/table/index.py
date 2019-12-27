import enum
from dataclasses import dataclass
from typing import List


class IndexType(enum.Enum):
    """
    Indexの種類
    """
    Primary = enum.auto()
    Local = enum.auto()
    Global = enum.auto()


@dataclass
class CapacityUnits:
    """
    CapacityUnitの詳細
    """
    read: int = 1
    write: int = 1

    def to_schema(self):
        """
        query用のスキーマを取得

        :return: dict
        """
        return {
            'ReadCapacityUnits': self.read,
            'WriteCapacityUnits': self.write
        }


class IndexBase(object):
    """
    Indexのベース
    """

    def __init__(self, name: str, hash_key: str, range_key: str = None):
        """
        init

        :param name: インデックス名
        :param hash_key: ハッシュキー名
        :param range_key: レンジキー名
        """
        self.name = name
        self.hash_key = hash_key
        self.range_key = range_key
        self.use_keys = {hash_key}
        if range_key is not None:
            self.use_keys.add(range_key)

    def check(self, hash_key: str, range_key: str = None):
        """
        使用できるかのチェック

        :param hash_key: ハッシュキー名
        :param range_key: レンジキー名
        :return: bool
        """

        # HASHのみで検索も可能なので、RANGEがNONEでも許容する
        if range_key is None:
            return self.hash_key == hash_key
        return self.hash_key == hash_key and self.range_key == range_key


class PrimaryIndex(IndexBase):
    """
    プライマリインデックス
    """

    def __init__(self, read_cap: int, write_cap: int, **kwargs):
        """
        init

        :param read_cap: リードキャパシティ
        :param write_cap: ライトキャパシティ
        :param kwargs:
        """
        super().__init__(**kwargs)
        self.capacity_units = CapacityUnits(read=read_cap, write=write_cap)


class SecondaryIndex(IndexBase):
    """
    セカンダリインデックス
    """

    def __init__(self, projection_keys: List[str], **kwargs):
        """
        init

        :param projection_keys: プロジェクションするキーリスト
        :param kwargs:
        """

        super().__init__(**kwargs)
        self.projection = projection_keys

    def to_projection_schema(self):
        """
        プロジェクションスキーマに変換

        :return: dict
        """
        if len(self.projection) > 0:
            return {
                'ProjectionType': 'INCLUDE',
                'NonKeyAttributes': self.projection
            }
        else:
            # TODO: ひとまず省略した場合はALLとする
            return {
                'ProjectionType': 'ALL',
            }


class GSI(SecondaryIndex):
    """
    グローバルセカンダリインデックス
    """

    def __init__(self, read_cap: int, write_cap: int, **kwargs):
        super().__init__(**kwargs)
        self.capacity_units = CapacityUnits(read=read_cap, write=write_cap)


class LSI(SecondaryIndex):
    """
    ローカルセカンダリインデックス
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

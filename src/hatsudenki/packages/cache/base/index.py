from typing import Optional


class PrimaryIndex(object):
    """
    インデックス情報
    """

    def __init__(self, hash_key: str, range_key: Optional[str] = None):
        """
        イニシャライザ

        :param hash_key: ハッシュキー名
        :param range_key: レンジキー名(ない場合はNone)
        """
        self.hash_key = hash_key
        self.range_key = range_key

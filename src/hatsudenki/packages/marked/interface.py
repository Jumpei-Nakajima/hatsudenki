class SerializableField(object):
    """
    DynamoDB形式にシリアライズ可能なフィールド
    """

    def serialize(self, data) -> dict:
        """
        DynamoDB形式にシリアライズ

        :param data: any
        :return: dict
        """
        pass

    def deserialize(self, data: dict):
        """
        Python形式にデシリアライズ

        :param data: DynamoDB形式のdict
        :return: any
        """
        pass

    @classmethod
    def is_empty(cls, value):
        return value is None


class Markable(object):
    """
    更新されたアトリビュートを保持可能なオブジェクト
    """

    def modify_mark(self, key):
        """
        更新されたことをマークする

        :param key: 更新されたキー
        :return: None
        """
        pass

    def flush(self):
        """
        変更情報をクリア

        :return: None
        """
        pass

from __future__ import annotations

import os
from collections import defaultdict
from os import PathLike
from pathlib import Path
from typing import Set, List, Dict

from hatsudenki.packages.command.files import yaml_load
from hatsudenki.packages.command.hatsudenki.field import HatsudenkiFieldBase, FieldFactory
from hatsudenki.packages.command.stdout.output import snake_to_camel


class HatsudenkiData(object):
    """
    Hatsudenkiデータスキーマ定義データ
    """

    class IndexInfo:
        """
        インデックス情報
        """

        def __init__(self):
            self.lsi: str = None
            self.gsi_hash: Set[str] = set()
            self.gsi_range: Set[str] = set()
            self.projection: Set[str] = set()

    def __init__(self, base_path: PathLike, full_path: PathLike, loader: any, *args, **kwargs):
        """
        イニシャライザ

        :param base_path: ベースパス
        :param full_path: フルパス
        :param loader: 自分を呼び出したローダ
        :param args:
        :param kwargs:
        """
        from .loader import HatsudenkiLoader
        self.full_path = Path(full_path)
        self.base_path = Path(base_path)
        self.data = yaml_load(self.full_path)
        # ファクトリを通してタイプに応じたフィールドクラスをインスタンス化した後に格納する
        self.attributes: Dict[str, HatsudenkiFieldBase] = {k: FieldFactory.create_field(k, v, self) for k, v in
                                                           self.data['attributes'].items()}

        self._table_name = None
        self._collection_name = None
        self._tag_name = None
        self.loader: HatsudenkiLoader = loader

    def _resolve_name(self):
        """
        テーブル名を解決

        :return: str
        """
        s = str(self.full_path.relative_to(self.base_path)).split('.')[0].replace(os.sep, '_')
        sp = s.split('_')
        if self.is_root:
            # rootテーブルはフォルダまでを基準とする
            sp.pop(-1)
        if len(sp) >= 2 and sp[-1] == sp[-2]:
            # 末尾2つの文字列が同一であった場合は省略する(user/user → user)
            sp = sp[:-1]
        return '_'.join(sp)

    @property
    def file_name(self):
        """
        ファイル名

        :return: str
        """
        return str(os.path.basename(self.full_path).split('.')[0])

    @property
    def is_root(self):
        """
        ルートテーブルか

        :return: boolean
        """
        return self.file_name == 'root'

    @property
    def is_single(self):
        """
        シングルテーブルか

        :return: boolean
        """
        return self.data.get('is_single', False)

    @property
    def is_alone(self):
        """
        ルートテーブル、もしくはシングルテーブルか

        :return: boolean
        """
        return self.is_root or self.is_single

    @property
    def label(self):
        """
        管理ツール用のテーブルかどうか
        :return:
        """
        return self.data.get('label', 'default')

    @property
    def parent_table(self):
        """
        親テーブルインスタンスを取得

        :return: 親テーブルインスタンス
        """
        if self.is_alone:
            # ルートテーブル、シングルテーブルには親が存在しない
            return None
        # 親を探す
        p = self.full_path.parent / 'root.yml'
        if self.loader.exists(p):
            return self.loader.get_from_path(p)

        # 存在しないエラー
        raise Exception('root not found.')

    @property
    def class_name(self):
        """
        ソース化したときのクラス名

        :return: str
        """
        return snake_to_camel(self.table_name)

    @property
    def collection_name(self):
        """
        コレクションネーム。シングルテーブルもしくはルートテーブルの場合はテーブルネームと同一。
        子テーブルの場合は親テーブルのテーブルネームとなる

        :return: str
        """
        if self._collection_name is None:
            if self.is_alone:
                # 単一テーブルの場合は自分の名前をそのまま返す
                self._collection_name = self.table_name
            else:
                # 子テーブルの場合は親の名前を返す
                self._collection_name = self.parent_table.collection_name
        return self._collection_name

    @property
    def table_name(self):
        """
        テーブル名

        :return: str
        """
        if self._table_name is None:
            self._table_name = self._resolve_name()
        return self._table_name

    @property
    def tag_name(self):
        """
        タグ名

        :return: str
        """
        if self.is_alone:
            # 単一テーブルはタグが存在しない
            return None

        if self._tag_name is None:
            # テーブル名から親テーブル名を削除したものをタグ文字列とする
            p = self.parent_table.table_name
            self._tag_name = self.table_name.replace(p + '_', '')
        return self._tag_name

    @property
    def indexes(self) -> List[dict]:
        """
        インデックス句

        :return: YAMLのIndex句
        """
        return self.data.get('indexes', [])

    @property
    def hash_key(self):
        """
        ハッシュキー

        :return: is_hash=Trueに設定されたattribute句の定義
        """
        return next((v for k, v in self.attributes.items() if v.is_hash), None)

    @property
    def range_key(self):
        """
        レンジキー

        :return: is_range=Trueに設定されたattribute句の定義
        """
        return next((v for k, v in self.attributes.items() if v.is_range), None)

    @property
    def capacity_units(self):
        """
        キャパシティユニット句

        :return: YAMLで定義されたcapacity_units句。省略された場合はリード、ライトともに1が入る。
        """
        return self.data.get('capacity_units', {'read': 1, 'write': 1})

    def parse_index(self):
        """
        インデックスを解釈し、適切なクラスをインスタンス化する。

        :return: IndexInfo, capacity_units
        """
        info = defaultdict(self.IndexInfo)
        units = {}

        for idx in self.indexes:
            # タイプ判定
            t = idx['type']
            if t == 'local':
                # LSIにはRANGEキーのみが設定されている。
                r = idx['range']
                l = f'_LSI-{r}'
                p = idx.get('projection', [])
                info[r].lsi = l
                [info[pk].projection.add(l) for pk in p]
            elif t == 'global':
                # GSIにはHASHとRANGEが存在し、キャパシティユニットが設定されている
                h = idx['hash']
                r = idx.get('range', '_')
                l = f'_GSI-{h}-{r}'
                p = idx.get('projection', [])
                info[h].gsi_hash.add(l)
                info[r].gsi_range.add(l)
                [info[pk].projection.add(l) for pk in p]
                units[l] = idx.get('capacity_units', {'read': 1, 'write': 1})
        return info, units

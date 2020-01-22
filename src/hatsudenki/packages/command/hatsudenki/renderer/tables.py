from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Tuple, Iterable

from hatsudenki.packages.command.hatsudenki.data import HatsudenkiData
from hatsudenki.packages.command.renderer.base import RenderUnit, FileRenderer
from hatsudenki.packages.command.stdout.output import IndentString


class TableRenderUnit(RenderUnit[HatsudenkiData]):
    """
    DynamoDBTableレンダラ
    """

    def __init__(self, data: HatsudenkiData):
        """
        イニシャライザ

        :param data: Dataインスタンス
        """
        super().__init__(data)
        # indexをパースしておく
        self.index_info, self.gsi_units = self.data.parse_index()

    @staticmethod
    def _render_opt_str(opt: dict):
        """
        dictをpythonのnamedparam形式文字列に変換する
        {xxx:yyy} -> xxx=yyy

        :param opt: 対象dict
        :return: str
        """
        return ', '.join(f'{k}={json.dumps(v) if json.dumps(v) != "null" else None}' for k, v in opt.items())

    def render_meta(self):
        """
        Metaクラスの出力

        :return: IndexStringインスタンス
        """
        table = self.data
        # hashキー及びrangeキーを取得
        hk = table.hash_key
        rk = table.range_key

        # 単一か子かによって継承元が変わる
        is_alone = table.is_alone
        if is_alone:
            # 単一テーブルの場合は普通に親から継承
            meta = IndentString('class Meta(SoloHatsudenkiTable.Meta):')
        else:
            # 子テーブルの場合はベースに加え、親テーブルのMetaも継承
            meta = IndentString(f'class Meta(ChildMultiHatsudenkiTable.Meta, {table.parent_table.class_name}.Meta):')

        # 共通属性
        meta.add(f'label = "{table.label}"')
        meta.add(f'is_root = {is_alone}')
        meta.add(f'table_name = "{table.table_name}"')

        # 単一テーブルか否かで処理を分岐
        if is_alone:
            # コレクション名
            meta.add(f'collection_name = "{table.collection_name}"')
            # キー情報
            opt = {
                'hash_key': hk.name
            }
            if rk is not None:
                # rangeキーが存在する場合は追加
                opt['range_key'] = rk.name

            # キャパシティユニット
            cap_unit = table.capacity_units
            opt['read_cap'] = cap_unit['read']
            opt['write_cap'] = cap_unit['write']
            # インデックス
            meta.add(f'primary_index = PrimaryIndex(name=None, {self._render_opt_str(opt)})')
        else:
            # 個別にクラスを出力する必要がある場合
            if rk:
                meta.add(rk.class_string)

            # TAGネーム
            meta.add(f'tag_name = "{table.tag_name}"')
            if rk is not None:
                # aliasキー
                meta.add(f'key_alias_name = "{rk.name}"')
                # aliasキータイプ
                meta.add(f'key_alias_type = {rk.def_python_field_class({})}')
        # 空行
        meta.blank_line()
        return meta

    def render_index(self):
        """
        インデックス情報の出力

        :return: IndexString
        """
        table = self.data
        # 単一か子かによって継承元が変わる
        is_alone = table.is_alone
        if is_alone:
            # 個別
            lsi_class_str = IndentString('class LSIndex:')
            gsi_class_str = IndentString('class GSIndex:')
        else:
            # 子
            pn = table.parent_table.class_name
            lsi_class_str = IndentString(f'class LSIndex({pn}.LSIndex):')
            gsi_class_str = IndentString(f'class GSIndex({pn}.LSIndex):')

        # インデックスは複数件あるのでループ
        for idx in table.indexes:
            opt = {}
            opt['projection_keys'] = idx.get('projection', [])
            # LSI or GSI
            if idx['type'] == 'global':
                # GSIはhashキーとrangeキーとキャパシティユニット
                opt['hash_key'] = idx['hash']
                # インデックス名はgsi__{hash}__{range}
                opt['name'] = f'gsi_{idx["hash"]}'
                if 'range' in idx:
                    opt['range_key'] = idx['range']
                    opt['name'] += '__' + idx['range']
                # キャパシティユニット
                caps = idx.get('capacity_units', {})
                if 'capacity_units' in caps:
                    c = caps['capacity_units']
                    opt['read_cap'] = c['read']
                    opt['write_cap'] = c['write']
                else:
                    opt['read_cap'] = 1
                    opt['write_cap'] = 1
                s = ', '.join((f'{k}={json.dumps(v)}' for k, v in opt.items()))
                gsi_class_str.add(f'{opt["name"]} = GSI({s})')
            else:
                # LSIはrangeキーのみ
                opt['name'] = f'lsi_{idx["range"]}'
                opt['range_key'] = idx['range']
                # 処理の都合上PrimaryのHashキーを便宜上のhashキーとして登録する
                if is_alone:
                    opt['hash_key'] = table.hash_key.name
                else:
                    # 子テーブルの場合はPrimaryキーを親テーブルから引く
                    opt['hash_key'] = table.parent_table.hash_key.name
                s = ', '.join((f'{k}={json.dumps(v)}' for k, v in opt.items()))
                lsi_class_str.add(f'{opt["name"]} = LSI({s})')

        # LSIが一つもない場合はpass
        if lsi_class_str.line_num is 1:
            lsi_class_str.add('pass')

        # GSIが一つもない場合はpass
        if gsi_class_str.line_num is 1:
            gsi_class_str.add('pass')
        body = IndentString()

        body.add(lsi_class_str, gsi_class_str)

        return body

    def render_fields(self):
        """
        Fieldクラスの出力

        :return: IndexString
        """
        table = self.data
        # 継承元の決定
        is_alone = table.is_alone
        if table.is_alone:
            body = IndentString('class Field(SoloHatsudenkiTable.Field):')
        else:
            body = IndentString(f'class Field({table.parent_table.class_name}.Field):')

        attr_num = 0
        for key, attr in table.attributes.items():
            if attr.is_alias_key:
                # aliasキーなので実際のフィールドとしては存在しない
                continue
            body.add(attr.class_string)
            body.add(attr.gen_def_str())

            attr_num += 1

        # 属性が一つもない場合はpass
        if attr_num is 0:
            body.add('pass')
        return body

    def render_init(self):
        """
        イニシャライザの出力

        :return: IndexString
        """
        table = self.data
        # キー情報の取得
        hk = table.hash_key
        rk = table.range_key
        keys = [k.name for k in [hk, rk] if k is not None]
        # hashキーとrangeキーだけ明示的にパラメータとして定義する
        o = ', '.join(
            [f'{k}: {table.attributes[k].python_init_type_str} = None' for k in keys])
        if len(o) > 0:
            o = f'{o}, '

        body = IndentString(f'def __init__(self, {o}**kwargs):')
        body.add('super().__init__(**kwargs)')
        # 記述を短くするためにFieldクラスの参照を保持する
        body.add('ft = self.__class__.Field')

        is_alone = table.is_alone
        if not is_alone and rk is None:
            # 子テーブル且つレンジキーがない場合は単一子テーブルとなる
            parent = table.parent_table
            # 単一子テーブルのrangeキーの値は常にtagで固定
            body.add(f'self.{parent.range_key.name} = "{table.tag_name}"')

        # 各フィールドの初期化処理
        for key, attr in table.attributes.items():
            body.add(attr.gen_init_str())

        return body

    def render_resolver(self):
        """
        リゾルバの出力

        :return: IndexString
        """
        # 今の所aliasキーのリゾルバしか無い
        body = IndentString()
        for k, attr in self.data.attributes.items():
            b = attr.resolver_string
            if b:
                body.add(b)

        return body

    def render(self):
        """
        出力

        :return: IndentString
        """
        table = self.data

        # 継承元の決定
        if table.is_alone:
            # solo or multi
            if table.range_key is not None:
                parent_cls_name = 'MultiHatsudenkiTable'
            else:
                parent_cls_name = 'SoloHatsudenkiTable'
        else:
            # solo or multi
            if table.range_key:
                parent_cls_name = f'{table.parent_table.class_name}, ChildMultiHatsudenkiTable'
            else:
                parent_cls_name = f'{table.parent_table.class_name}, ChildSoloHatsudenkiTable'

        # クラス定義
        cls = IndentString(f'class {table.class_name}({parent_cls_name}):')

        # Metaクラス
        cls.add(self.render_meta())

        cls.blank_line()

        cls.add(self.render_fields())
        cls.blank_line()

        # Index
        cls.add(self.render_index())
        cls.blank_line()
        # init
        cls.add(self.render_init())
        cls.blank_line()
        # resolver
        cls.add(self.render_resolver())

        cls.blank_line()

        return cls


class TableFileRenderer(FileRenderer):
    """
    ファイルレンダラ
    """

    def __init__(self, table_iterator: Iterable[Tuple[Path, HatsudenkiData]], out_module_name: str):
        """
        イニシャライザ

        :param table_iterator: Dataのイテレータ
        """
        super().__init__()

        # rootとchildに切り分ける
        d = {'root': [], 'child': []}
        [d['root' if v.is_alone else 'child'].append(v) for k, v in table_iterator]

        # rootから先に書く
        [self.add_unit(TableRenderUnit(t)) for t in d['root']]
        [self.add_unit(TableRenderUnit(t)) for t in d['child']]

        self.out_module_name = out_module_name.replace(os.sep, '.')

    def render_header(self) -> IndentString:
        """
        ヘッダ出力

        :return: IndentString
        """
        # インポートするモジュール群
        d = IndentString()
        l = [
            'from __future__ import annotations',
            'import uuid',
            f'from {self.out_module_name} import masters',
            'from datetime import datetime',
            f'from {self.out_module_name} import def_enum',
            'from hatsudenki.packages import field',
            'from hatsudenki.packages.marked import MarkedObject, Markable, MarkedObjectWithIndex',
            'from hatsudenki.packages.table.index import PrimaryIndex, LSI, GSI',
            'from hatsudenki.packages.table.multi import MultiHatsudenkiTable',
            'from hatsudenki.packages.table.solo import SoloHatsudenkiTable',
            'from hatsudenki.packages.table.child import ChildMultiHatsudenkiTable',
            'from hatsudenki.packages.table.child_solo import ChildSoloHatsudenkiTable',
        ]
        d.add(*l)
        d.blank_line(2)

        d.indent('def table_setup():')
        d.add('from hatsudenki.packages.manager.table import TableManager')
        d.add(
            'print(f"load all tables OK. collections={TableManager.get_collection_num()}  tables={TableManager.get_table_num()}")')
        d.blank_line(2)

        return d

import datetime
from dataclasses import dataclass, field
from logging import getLogger
from typing import Type, Dict, List

from botocore.exceptions import ClientError

from hatsudenki.packages.client import HatsudenkiClient
from hatsudenki.packages.table.index import PrimaryIndex, GSI, LSI, IndexBase
from hatsudenki.packages.table.solo import SoloHatsudenkiTable

_logger = getLogger(__name__)


@dataclass
class CollectionIndex:
    primary: PrimaryIndex
    gsi: List[GSI] = field(default_factory=list)
    lsi: List[LSI] = field(default_factory=list)


class DateResolver(object):
    def get_now(self):
        raise NotImplementedError()


class DefaultDateResolver(DateResolver):
    def get_now(self):
        return datetime.datetime.now()


class TableManager(object):
    _all_collections: Dict[str, List[SoloHatsudenkiTable]] = {}
    _all_tables: Dict[str, SoloHatsudenkiTable] = {}
    _indexes: Dict[str, CollectionIndex] = {}

    @classmethod
    def register(cls, tbl: Type[SoloHatsudenkiTable]):
        tbl_name = tbl.Meta.table_name
        if tbl_name is None or len(tbl_name) is 0:
            return False

        if tbl_name in cls._all_tables:
            return False
        cls._all_tables[tbl.get_table_name()] = tbl
        c = cls._all_collections.get(tbl.Meta.collection_name, [])
        c.append(tbl)
        cls._all_collections[tbl.Meta.collection_name] = c

        i = cls._indexes.get(tbl.Meta.collection_name, None)
        if i is None:
            i = CollectionIndex(tbl.Meta.primary_index, [], [])
            cls._indexes[tbl.Meta.collection_name] = i

        for v in tbl.LSIndex.__dict__.values():
            if type(v) is LSI:
                i.lsi.append(v)
        for v in tbl.GSIndex.__dict__.values():
            if type(v) is GSI:
                i.gsi.append(v)
        return True

    @classmethod
    def get_by_table_name(cls, table_name: str):
        return cls._all_tables[table_name]

    @classmethod
    def get_index(cls, collection_name: str):
        return cls._indexes[collection_name]

    @classmethod
    def find_field_class(cls, collectioln_name: str, key_name: str):
        i = cls._all_collections[collectioln_name]
        for t in i:
            f = t.get_field_class(key_name)
            if f:
                return f
        return None

    @classmethod
    def _to_key_schema(cls, tbl: SoloHatsudenkiTable, idx: IndexBase):
        i = cls._all_collections[tbl.Meta.collection_name]
        ks = []
        h = cls.find_field_class(tbl.get_collection_name(), idx.hash_key)
        ks.append(h.key_schema('HASH'))
        if idx.range_key is not None:
            print(f'{tbl.get_collection_name()}-{tbl.get_table_name()} {idx.range_key}')
            r = cls.find_field_class(tbl.get_collection_name(), idx.range_key)
            ks.append(r.key_schema('RANGE'))

        return ks

    @classmethod
    async def drop_table(cls, collection_name: str):
        await HatsudenkiClient.drop_table(collection_name)

    @classmethod
    async def all_drop(cls):
        for name, tbls in cls._all_collections.items():
            for tbl in tbls:
                if not tbl.Meta.is_root:
                    continue
                try:
                    await cls.drop_table(tbl.get_collection_name())
                except ClientError as e:
                    # テーブル無いエラーは無視して構わない
                    if e.response['Error']['Code'] != 'ResourceNotFoundException':
                        raise e

    @classmethod
    async def create_tables(cls, drop=False, labels=['default']):
        _logger.info('begin create table.')
        for name, tbls in cls._all_collections.items():
            for tbl in tbls:
                if tbl.Meta.label not in labels:
                    continue
                if not tbl.Meta.is_root:
                    continue
                _logger.info(f'create {name}')
                print(f'create {name}')

                attrs = set()
                idx = cls._indexes[tbl.Meta.collection_name]
                key_schema = cls._to_key_schema(tbl, idx.primary)
                attrs |= idx.primary.use_keys

                lsi = []
                for l in idx.lsi:
                    s = cls._to_key_schema(tbl, l)
                    lsi.append({
                        'IndexName': l.name,
                        'KeySchema': s,
                        'Projection': l.to_projection_schema()
                    })

                    attrs |= l.use_keys
                gsi = []
                for g in idx.gsi:
                    # GSIの生成
                    s = cls._to_key_schema(tbl, g)
                    gsi.append({
                        'IndexName': g.name,
                        'KeySchema': s,
                        'ProvisionedThroughput': g.capacity_units.to_schema(),
                        'Projection': g.to_projection_schema()
                    })
                    attrs |= g.use_keys

                # 使用キーは定義リストに入っている必要がある
                attr_def = [cls.find_field_class(tbl.get_collection_name(), key).attribute_define for key in attrs]

                if drop:
                    _logger.warning(f'drop table {name}')
                    # 作る前にドロップする
                    try:
                        await cls.drop_table(tbl.get_collection_name())
                    except ClientError as e:
                        # テーブル無いエラーは無視して構わない
                        if e.response['Error']['Code'] != 'ResourceNotFoundException':
                            raise e
                try:
                    # 作る
                    res = await HatsudenkiClient.create_table(
                        tbl.get_collection_name(), attr_def, idx.primary.capacity_units.to_schema(), key_schema, lsi,
                        gsi)
                except ClientError as e:
                    # テーブルすでにあるエラーは無視して構わない
                    if e.response['Error']['Code'] != 'ResourceInUseException':
                        raise e

                    # Index情報を合わせる
                    await HatsudenkiClient.update_table(
                        tbl.get_collection_name(), attr_def,
                        idx.primary.capacity_units.to_schema(), key_schema, lsi,
                        gsi
                    )

                    res = None

                # TTLが設定されているか
                ttl_key = next((k for k, v in tbl._attributes.items() if v.is_ttl), None)
                if ttl_key is not None:
                    try:
                        await HatsudenkiClient.set_ttl_mode(tbl.get_collection_name(), ttl_key, True)
                    except:
                        pass

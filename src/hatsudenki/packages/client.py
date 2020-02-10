import asyncio
import json
import os
import time
from asyncio import sleep
from copy import deepcopy
from datetime import date, datetime
from logging import getLogger
from random import choice
from typing import List, AsyncGenerator, IO

from aioboto3 import Session
from botocore.config import Config

from hatsudenki.packages.counter import QueryCounter
from hatsudenki.packages.expression.base import BaseExpression
from hatsudenki.packages.expression.condition import ConditionExpression, KeyConditionExpression, \
    FilterConditionExpression
from hatsudenki.packages.expression.update import UpdateExpression

_logger = getLogger(__name__)


class HatsudenkiClient(object):
    """
    DynamoDBクライアント
    """

    #: クライアントインスタンス
    _clients = []
    _prefix: str = None
    _is_out_slow_log = False
    _slow_log_duration = 0.1
    _use_profiler = False

    @classmethod
    def dump_count(cls):
        if cls._use_profiler:
            QueryCounter.dump_counter()

    @classmethod
    def setup_by_session(cls, session, prefix: str, endpoint: str = None, connection_num: int = 10):
        con = Config(
            parameter_validation=False,
            max_pool_connections=1,
        )

        print(f'connect dynamodb. endpoint={endpoint} prefix={prefix}, connection_num={connection_num}')
        for _ in range(connection_num):
            cls._clients.append(session.client('dynamodb', endpoint_url=endpoint, config=con, use_ssl=False))
        cls._prefix = prefix

    @classmethod
    def setup(cls, loop, endpoint: str, prefix: str = '', region_name: str = None, aws_access_key_id: str = None,
              aws_secret_access_key: str = None, pool_num: int = 10):
        """
        | 初期設定を行う。
        | すべての処理より先に一度だけ呼び出すこと

        :param loop: 処理を行うIOループ
        :param endpoint: エンドポイント
        :param prefix: テーブルのプリフィックス
        :return: None
        """

        if endpoint:
            # endpointが渡された場合はローカルモードとする
            region = 'local'
            acc_id = 'hogehoge'
            secret = 'fugafuga'
        else:
            region = region_name or os.getenv('AWS_REGION', 'ap-northeast-1')
            acc_id = aws_access_key_id or os.getenv('AWS_ACCESS_KEY_ID')
            secret = aws_secret_access_key or os.getenv('AWS_SECRET_ACCESS_KEY')

        d = {
            'loop': loop
        }

        if region:
            d['region_name'] = region
        if acc_id:
            d['aws_access_key_id'] = acc_id
        if secret:
            d['aws_secret_access_key'] = secret

        ses = Session(
            **d
        )

        cls.setup_by_session(session=ses, prefix=prefix, endpoint=endpoint, connection_num=pool_num)

    @classmethod
    def set_slow_log(cls, flg: bool, duration: float):
        cls._is_out_slow_log = flg
        cls._slow_log_duration = duration

    @classmethod
    def set_use_profiler(cls, flg: bool):
        cls._use_profiler = flg

    @classmethod
    def _take(cls):
        if cls._is_out_slow_log:
            return time.time()
        return None

    @classmethod
    def _cheese(cls, sec, label: str, key: dict):
        if cls._is_out_slow_log:
            p = time.time() - sec
            if p >= cls._slow_log_duration:
                _logger.warning(f'[SLOW] {label} sec={p}', key)
        else:
            return

    @classmethod
    def _get_client(cls):
        return choice(cls._clients)

    @classmethod
    def resolve_table_name(cls, table_name: str, skip: bool = False):
        if skip:
            return table_name
        return cls._prefix + table_name

    @classmethod
    async def create_table(cls, table_name: str, attributes: List[dict], capacity_units: dict, key_schema: List[dict],
                           lsi: List[dict] = None, gsi: List[dict] = None):
        """
        | テーブルを作る
        | DynamoDBはテーブル作成時、キー以外の情報は不要（渡すとエラーになる）

        :param table_name: 対象テーブル名
        :param attributes: 定義する属性
        :param capacity_units: デプロイするキャパシティユニット
        :param key_schema: キー情報
        :param lsi: LocalSecondaryIndex
        :param gsi: GlobalSecondaryIndex
        :return: AWSレスポンス
        """
        ondemand = False
        if capacity_units['ReadCapacityUnits'] == 1 and capacity_units['WriteCapacityUnits'] == 1:
            p = {
                'TableName': cls.resolve_table_name(table_name),
                'AttributeDefinitions': attributes,
                'KeySchema': key_schema,
                'BillingMode': 'PAY_PER_REQUEST'
            }
            ondemand = True
        else:
            p = {
                'TableName': cls.resolve_table_name(table_name),
                'AttributeDefinitions': attributes,
                'KeySchema': key_schema,
                'ProvisionedThroughput': capacity_units
            }

        if lsi is not None and len(lsi) > 0:
            p['LocalSecondaryIndexes'] = lsi

        if gsi is not None and len(gsi) > 0:
            if ondemand:
                gsi = deepcopy(gsi)
                for g in gsi:
                    del g['ProvisionedThroughput']
            p['GlobalSecondaryIndexes'] = gsi

        res = await cls._get_client().create_table(**p)

        return res

    @classmethod
    async def update_table(cls, table_name: str, attributes: List[dict], capacity_units: dict, key_schema: List[dict],
                           lsi: List[dict] = None, gsi: List[dict] = None):
        """
        テーブルを更新する(インデックスとかつくる)
        ※DynamoDBの制限
        - 基本的にGSIの追加・削除およびキャパシティユニットの増減しか行なえません。
        - GSIは追加・削除が行えますが、変更はできません（スループットの変更のみ可能）
        - GSIの操作は同時に1インデックスずつしか行なえません（このメソッドは内部は完了を待ちます）

        :param table_name: 対象テーブル名
        :param attributes: 定義する属性
        :param capacity_units: デプロイするキャパシティユニット
        :param key_schema: キー情報
        :param lsi: LocalSecondaryIndex
        :param gsi: GlobalSecondaryIndex
        :return:
        """

        def _compare_key(old_keys, new_keys):
            old_key_schema = {k['KeyType']: k['AttributeName'] for k in old_keys}
            new_key_schema = {k['KeyType']: k['AttributeName'] for k in new_keys}
            if old_key_schema['HASH'] != new_key_schema['HASH'] \
                    or old_key_schema.get('RANGE', None) != new_key_schema.get('RANGE', None):
                return False
            return True

        def _compare_proj(old_proj, new_proj):
            if old_proj['ProjectionType'] != new_proj['ProjectionType']:
                return False
            if old_proj['ProjectionType'] == 'INCLUDE':
                if set(old_proj['NonKeyAttributes']) != set(new_proj['NonKeyAttributes']):
                    return False
            return True

        # 現在の状態を取得
        desc = await cls.describe_table(table_name)

        # PrimaryKeyは変更できない
        if not _compare_key(desc['KeySchema'], key_schema):
            raise Exception(f'PrimaryIndexが変更されています Table={table_name}')

        # LSIの検証
        now_lsi = {k['IndexName']: k for k in desc.get('LocalSecondaryIndexes', [])}
        new_lsi = {k['IndexName']: k for k in lsi}

        if now_lsi.keys() != new_lsi.keys():
            raise Exception(f'LocalSecondaryIndexはあとから変更できません Table={table_name}')

        for n, i in new_lsi.items():
            li = now_lsi[n]
            if not _compare_key(li['KeySchema'], i['KeySchema']):
                raise Exception(f'LocalSecondaryIndexはあとから変更できません Table={table_name}')
            if not _compare_proj(li['Projection'], i['Projection']):
                raise Exception(f'LocalSecondaryIndexはあとから変更できません Table={table_name}')

        # GSIの差分確認
        now_gsi = {k['IndexName']: k for k in desc.get('GlobalSecondaryIndexes', [])}
        new_gsi = {k['IndexName']: k for k in gsi}

        # 変更の検知
        for k in new_gsi.keys() & now_gsi.keys():
            nk = new_gsi[k]
            ok = now_gsi[k]
            if not _compare_key(ok['KeySchema'], nk['KeySchema']):
                raise Exception(f'GlobalSecondaryIndexはあとから変更できません Table={table_name}')
            if not _compare_proj(ok['Projection'], nk['Projection']):
                raise Exception(f'GlobalSecondaryIndexはあとから変更できません Table={table_name}')

        # 追加の検知
        add_gsi = new_gsi.keys() - now_gsi.keys()
        # 削除の検知
        del_gsi = now_gsi.keys() - new_gsi.keys()

        key_schema_keys = set([k['AttributeName'] for k in key_schema])
        now_attributes = {d['AttributeName']: d for d in desc['AttributeDefinitions']}
        for del_key in del_gsi:
            del_index = now_gsi[del_key]
            del_index_keys = [k['AttributeName'] for k in del_index['KeySchema']]

            atr = [now_attributes[ak] for ak in (key_schema_keys | set(del_index_keys))]

            p = {
                'TableName': cls.resolve_table_name(table_name),
                'AttributeDefinitions': atr,
                'GlobalSecondaryIndexUpdates': [
                    {
                        'Delete': {'IndexName': del_key}
                    }
                ],
            }
            await cls._get_client().update_table(**p)

            # 完了を待つ
            for _ in range(60):
                _desc = await cls.describe_table(table_name)
                _gsi = _desc.get('GlobalSecondaryIndexes', [])
                target = next((g['IndexStatus'] for g in _gsi if g['IndexName'] == del_key), None)
                if target is None:
                    break
                await sleep(1)

        # GSIの作成は一回ずつしかできない
        is_ondemand = desc.get('BillingModeSummary') and desc.get('BillingModeSummary')[
            'BillingMode'] == 'PAY_PER_REQUEST'

        work_attributes = {k['AttributeName']: k for k in attributes}
        for add_key in add_gsi:
            add_index = new_gsi[add_key]
            if is_ondemand and add_index.get('ProvisionedThroughput'):
                # ondemand時はCUの設定は不要
                del add_index['ProvisionedThroughput']

            add_index_keys = [k['AttributeName'] for k in add_index['KeySchema']]

            atr = [work_attributes[ak] for ak in (key_schema_keys | set(add_index_keys))]

            p = {
                'TableName': cls.resolve_table_name(table_name),
                'AttributeDefinitions': atr,
                'GlobalSecondaryIndexUpdates': [
                    {
                        'Create': add_index
                    }
                ],
            }

            await cls._get_client().update_table(**p)

            # 完了を待つ
            for _ in range(60):
                _desc = await cls.describe_table(table_name)
                _gsi = _desc['GlobalSecondaryIndexes']
                target = next((g['IndexStatus'] for g in _gsi if g['IndexName'] == add_key), None)
                if target is None:
                    raise Exception('Invalid GSI!!!')
                if target == 'ACTIVE':
                    break
                await sleep(1)

    @classmethod
    async def drop_table(cls, table_name: str):
        _start = cls._take()
        p = {
            'TableName': cls.resolve_table_name(table_name)
        }
        res = await cls._get_client().delete_table(**p)
        cls._cheese(_start, f'drop_table {table_name}', p)
        return res

    @classmethod
    async def describe_table(cls, table_name: str):
        """
        テーブル情報を取得

        :param table_name: 対象テーブル名
        :return: テーブル情報を格納した辞書配列。AWSのレスポンス参照
        """

        _start = cls._take()
        p = {
            'TableName': cls.resolve_table_name(table_name)
        }
        res = await cls._get_client().describe_table(**p)
        cls._cheese(_start, f'describe_table {table_name}', p)
        return res['Table']

    @classmethod
    async def list_tables(cls, start_table_name: str = None) -> List[str]:
        """
        テーブル一覧を取得

        :return: テーブル名のリスト
        """
        _start = cls._take()
        st = cls._prefix or ''

        if start_table_name:
            st += start_table_name

        p = {
            **({'ExclusiveStartTableName': st} if len(st) is not 0 else {})
        }

        res = await cls._get_client().list_tables(**p)
        cls._cheese(_start, f'list_tables {start_table_name}', p)
        return [i.replace(st, '') for i in res.get('TableNames')]

    @classmethod
    async def get_item(cls, table_name: str, key: dict, prj: List[str] = None):
        """
        キー指定式を使用して一件取得

        :param table_name: テーブル名
        :param key: キー指定式辞書配列
        :return: アイテム情報を格納した辞書配列。AWSレスポンス参照。アイテムが存在しない場合はNone
        """

        _start = cls._take()
        p = {
            'TableName': cls.resolve_table_name(table_name),
            'Key': key
        }

        if prj is not None:
            p['ProjectionExpression'] = ','.join(prj)

        res = await cls._get_client().get_item(**p)

        if cls._use_profiler:
            p['ReturnConsumedCapacity'] = 'INDEXES'

        r = res.get('Item', None)

        cls._cheese(_start, f'get_item, {table_name}', p)
        if cls._use_profiler:
            if key.get('kind', None):
                QueryCounter.count('get', f"{table_name} - {key['kind']['S'].split('//')[0]}")
            else:
                QueryCounter.count('get', f"{table_name}")

            consumed_cu = res.get('ConsumedCapacity', False)
            if consumed_cu:
                QueryCounter.count_read_ccu(consumed_cu)

        return r

    @classmethod
    async def put_item(cls, table_name: str, item: dict, condition: ConditionExpression = None, raw_table_name=False):
        """
        | アイテムを作成
        | 更新条件式が成立しなかった場合は例外が発生する

        :param table_name: 対象テーブル名
        :param item: シリアライズされたアイテム情報を格納した辞書配列
        :param condition: 更新条件式インスタンス
        :param raw_table_name: テーブル名にプリフィックスを付与しない
        :return: AWSレスポンス
        """
        _start = cls._take()

        p = {
            'TableName': cls.resolve_table_name(table_name, raw_table_name),
            'Item': item,

            **(condition.to_parameter() if condition is not None else {})
        }
        if cls._use_profiler:
            p['ReturnConsumedCapacity'] = 'INDEXES'

        res = await cls._get_client().put_item(**p)

        cls._cheese(_start, f'put_item {table_name}', p)
        if cls._use_profiler:
            if item.get('kind', None):
                QueryCounter.count('put', f"{table_name} - {item['kind']['S'].split('//')[0]}")
            else:
                QueryCounter.count('put', f"{table_name}")

            consumed_cu = res.get('ConsumedCapacity', False)
            if consumed_cu:
                QueryCounter.count_write_ccu(consumed_cu)

        return res

    @classmethod
    async def delete_item(cls, table_name, key: dict, condition: ConditionExpression = None, raw_table_name=False):
        _start = cls._take()

        p = {
            'TableName': cls.resolve_table_name(table_name, raw_table_name),
            'Key': key,
            **(condition.to_parameter() if condition is not None else {})
        }

        res = await cls._get_client().delete_item(**p)

        cls._cheese(_start, f'delete_item {table_name}', p)
        return res

    @classmethod
    async def update_item(cls, table_name: str, key: dict, update: UpdateExpression,
                          condition: ConditionExpression = None, raw_table_name=False):
        """
        アイテムを更新

        :param table_name: 対象テーブル名
        :param key: ハッシュキー及びレンジキーの指定式
        :param update: 更新値式インスタンス
        :param condition: 更新条件式インスタンス
        :return: AWSレスポンス
        """
        _start = cls._take()

        p = {
            'TableName': cls.resolve_table_name(table_name, raw_table_name),
            'Key': key,
            **(update.to_parameter(condition))
        }

        if cls._use_profiler:
            p['ReturnConsumedCapacity'] = 'INDEXES'
        res = await cls._get_client().update_item(**p)

        cls._cheese(_start, f'update_item {table_name}', p)
        if cls._use_profiler:
            if key.get('kind', None):
                QueryCounter.count('update', f"{table_name} - {key['kind']['S'].split('//')[0]}")
            else:
                QueryCounter.count('update', f"{table_name}")
            consumed_cu = res.get('ConsumedCapacity', False)
            if consumed_cu:
                QueryCounter.count_write_ccu(consumed_cu)
        return res

    @classmethod
    async def query(cls, table_name: str, key_cond: KeyConditionExpression, filter_cond: BaseExpression = None,
                    use_index_name: str = None, limit=0, prj: List[str] = None, raw_table_name=False):
        """
        条件式を複数指定する検索。複数件のアイテムを返却する

        :param table_name: 対象テーブル名
        :param key_cond: キー絞り込み
        :param filter_cond: フィルター
        :param use_index_name: 使用するインデックス名
        :param limit: リミット
        :param prj: プロジェクション情報
        :param raw_table_name: テーブル名にプリフィックスを付与しない
        :return: AWSレスポンス
        """

        _start = cls._take()

        p = {
            'TableName': cls.resolve_table_name(table_name, raw_table_name),
            **({'IndexName': use_index_name} if use_index_name is not None else {}),
            **({'Limit': limit} if limit > 0 is not None else {}),
            **(BaseExpression.merge(key_cond, filter_cond))
        }
        if prj is not None:
            p['ProjectionExpression'] = ','.join(prj)

        if cls._use_profiler:
            p['ReturnConsumedCapacity'] = 'INDEXES'

        res = await cls._get_client().query(**p)

        cls._cheese(_start, f'query {table_name}', p)

        if cls._use_profiler:
            if p['ExpressionAttributeValues'].get(':key_value__1', None):
                QueryCounter.count('query',
                                   f"{table_name} - {p.get('IndexName')} - {p['ExpressionAttributeValues'][':key_value__1']}")
            else:
                QueryCounter.count('query', f"{table_name}")
            consumed_cu = res.get('ConsumedCapacity', False)
            if consumed_cu:
                QueryCounter.count_read_ccu(consumed_cu)
        return res['Items']

    @classmethod
    async def scan(cls, table_name: str, filter_cond: FilterConditionExpression = None, limit=20,
                   prj: List[str] = None, raw_table_name=False):
        """
        フルスキャンを行う。危険なので使用には気をつけること。

        :param table_name: 対象テーブル名
        :param filter_cond: 絞り込み条件式
        :return: AWSレスポンス
        """
        _start = cls._take()
        p = {
            'TableName': cls.resolve_table_name(table_name, raw_table_name),
            **(filter_cond.to_parameter() if filter_cond is not None else {}),
            'Limit': limit
        }
        if prj is not None:
            p['ProjectionExpression'] = ','.join(prj)

        res = await cls._get_client().scan(**p)
        ret = res['Items']
        cls._cheese(_start, f'scan {table_name}', p)
        if len(ret) >= limit:
            return ret[:limit]

        while 'LastEvaluatedKey' in res:
            _start = cls._take()
            p['ExclusiveStartKey'] = res['LastEvaluatedKey']
            res = await cls._get_client().scan(**p)
            ret.extend(res['Items'])
            cls._cheese(_start, f'scan {table_name}', p)
            if len(ret) >= limit:
                return ret[:limit]

        return ret

    @classmethod
    async def scan_generator(cls, table_name: str, filter_cond: FilterConditionExpression = None, tick=100,
                             prj: List[str] = None, raw_table_name=False) -> AsyncGenerator[List[dict], None]:

        p = {
            'TableName': cls.resolve_table_name(table_name, raw_table_name),
            **(filter_cond.to_parameter() if filter_cond is not None else {}),
            'Limit': tick
        }
        if prj is not None:
            p['ProjectionExpression'] = ','.join(prj)

        res = await cls._get_client().scan(**p)
        ret = res['Items']
        yield ret

        if 'LastEvaluatedKey' not in res:
            return

        while 'LastEvaluatedKey' in res:
            p['ExclusiveStartKey'] = res['LastEvaluatedKey']
            res = await cls._get_client().scan(**p)
            yield res['Items']

    @classmethod
    async def set_ttl_mode(cls, table_name: str, attr_name: str, set_mode=True, raw_table_name=False):
        _start = cls._take()
        p = {
            'TableName': cls.resolve_table_name(table_name),
            'TimeToLiveSpecification': {
                'Enabled': set_mode,
                'AttributeName': attr_name
            }
        }
        res = await cls._get_client().update_time_to_live(**p)

        cls._cheese(_start, f'set_ttl_mode {table_name}', p)
        return res

    @classmethod
    async def batch_get_item(cls, request_items: dict):

        _start = cls._take()
        p = {
            'RequestItems': request_items
        }
        res = await cls._get_client().batch_get_item(**p)
        cls._cheese(_start, 'batch_get_item', p)

        if cls._use_profiler:
            p['ReturnConsumedCapacity'] = 'INDEXES'
        res = await cls._get_client().batch_get_item(**p)
        cls._cheese(_start, 'batch_get_item', p)

        if cls._use_profiler:
            QueryCounter.count('batch_get')
            consumed_cu_list = res.get('ConsumedCapacity', False)
            for consumed_cu in consumed_cu_list:
                QueryCounter.count_read_ccu(consumed_cu)

        return res['Responses']

    @classmethod
    async def batch_write_item(cls, request_items: dict):
        _start = cls._take()
        p = {
            'RequestItems': request_items
        }
        res = await cls._get_client().batch_write_item(**p)
        cls._cheese(_start, 'batch_write_item', p)

        if cls._use_profiler:
            p['ReturnConsumedCapacity'] = 'INDEXES'
        res = await cls._get_client().batch_write_item(**p)
        cls._cheese(_start, 'batch_write_item', p)
        if cls._use_profiler:
            QueryCounter.count('batch_write')
            consumed_cu_list = res.get('ConsumedCapacity', False)
            for consumed_cu in consumed_cu_list:
                QueryCounter.count_write_ccu(consumed_cu)
        return res['UnprocessedItems']

    @classmethod
    async def transaction_write(cls, items: list):
        _start = cls._take()
        p = {
            'TransactItems': items
        }

        res = await cls._get_client().transact_write_items(**p)
        cls._cheese(_start, 'transact_write', p)
        if cls._use_profiler:
            p['ReturnConsumedCapacity'] = 'INDEXES'
        res = await cls._get_client().transact_write_items(**p)
        cls._cheese(_start, 'transact_write', p)

        if cls._use_profiler:
            QueryCounter.count('transact_write')
            consumed_cu_list = res.get('ConsumedCapacity', False)
            for consumed_cu in consumed_cu_list:
                QueryCounter.count_write_ccu(consumed_cu)
        return res

    @classmethod
    async def transaction_get(cls, items: list) -> List[dict]:

        _start = cls._take()
        p = {
            'TransactItems': items
        }

        res = await cls._get_client().transact_get_items(**p)
        cls._cheese(_start, 'transact_get', p)

        if cls._use_profiler:
            p['ReturnConsumedCapacity'] = 'INDEXES'

        res = await cls._get_client().transact_get_items(**p)
        cls._cheese(_start, 'transact_get', p)
        if cls._use_profiler:
            QueryCounter.count('transact_get')
            consumed_cu_list = res.get('ConsumedCapacity', False)
            for consumed_cu in consumed_cu_list:
                QueryCounter.count_write_ccu(consumed_cu)
        return res['Responses']

    def __init__(self):
        # インスタンス化する必要はない
        raise Exception('TatsumakiClient is mono state.')

    @classmethod
    async def die(cls):
        t = [c.close() for c in cls._clients]
        await asyncio.gather(*t)

    @classmethod
    async def export_json(cls, table_name: str, out_stream: IO, tick=100, raw_table_name=False):
        def json_serial(obj):
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            raise TypeError("Type %s not serializable" % type(obj))

        desc = await cls.describe_table(table_name)
        out_stream.write(json.dumps(desc, ensure_ascii=False, default=json_serial) + '\n')

        gen = cls.scan_generator(table_name=table_name, filter_cond=None, tick=tick, prj=None,
                                 raw_table_name=raw_table_name)
        async for l in gen:
            for i in l:
                out_stream.write(json.dumps(i, ensure_ascii=False) + '\n')

    @classmethod
    async def import_json(cls, table_name: str, in_stream: IO, recreate=False):
        desc = json.loads(in_stream.readline())
        if recreate:
            p = {
                'TableName': cls.resolve_table_name(table_name),
                'AttributeDefinitions': desc['AttributeDefinitions'],
                'KeySchema': desc['KeySchema']
            }

            if desc['ProvisionedThroughput']:
                p['ProvisionedThroughput'] = {
                    'ReadCapacityUnits': desc['ProvisionedThroughput']['ReadCapacityUnits'],
                    'WriteCapacityUnits': desc['ProvisionedThroughput']['WriteCapacityUnits']
                }
            else:
                p['BillingMode'] = 'PAY_PER_REQUEST'

            if 'GlobalSecondaryIndexes' in desc:
                gsi = []
                for g in desc['GlobalSecondaryIndexes']:
                    c = {
                        'IndexName': g['IndexName'],
                        'KeySchema': g['KeySchema'],
                        'Projection': g['Projection']
                    }
                    if 'ProvisionedThroughput' in g:
                        c['ProvisionedThroughput'] = {
                            'ReadCapacityUnits': g['ProvisionedThroughput']['ReadCapacityUnits'],
                            'WriteCapacityUnits': g['ProvisionedThroughput']['WriteCapacityUnits']
                        }
                    gsi.append(c)
                p['GlobalSecondaryIndexes'] = gsi

            if 'LocalSecondaryIndexes' in desc:
                lsi = []
                for l in desc['LocalSecondaryIndexes']:
                    c = {
                        'IndexName': l['IndexName'],
                        'KeySchema': l['KeySchema'],
                        'Projection': l['Projection']
                    }
                    lsi.append(c)
                p['LocalSecondaryIndexes'] = lsi

            # 作り直す
            await cls.drop_table(table_name)
            await cls._get_client().create_table(**p)

        idx = 0
        items = []
        for line in in_stream:
            d = json.loads(line)
            items.append({'PutRequest': {'Item': d}})
            idx += 1
            if idx >= 25:
                await cls.batch_write_item(
                    request_items={cls.resolve_table_name(table_name): items}
                )
                items = []

        if len(items) > 0:
            await cls.batch_write_item(
                request_items={cls.resolve_table_name(table_name): items}
            )

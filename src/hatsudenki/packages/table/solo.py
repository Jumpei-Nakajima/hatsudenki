from logging import getLogger
from typing import Type, TypeVar, List, Dict, Tuple, Generator, Callable, Optional, AsyncGenerator

from hatsudenki.packages.client import HatsudenkiClient
from hatsudenki.packages.expression.condition import ConditionExpression, KeyConditionExpression, \
    FilterConditionExpression
from hatsudenki.packages.expression.update import UpdateExpression
from hatsudenki.packages.field import NumberField
from hatsudenki.packages.field.base import BaseHatsudenkiField
from hatsudenki.packages.field.extra import CreateDateField, UpdateDateField
from hatsudenki.packages.manager.date import DateManager
from hatsudenki.packages.table.define import TableType
from hatsudenki.packages.table.index import PrimaryIndex

T = TypeVar('T')

_logger = getLogger(__name__)


class SoloHatsudenkiTable(object):
    # テーブルメタ情報定義
    class Meta:
        label = 'default'
        is_root = True
        # テーブル名
        collection_name = ''
        primary_index: PrimaryIndex = None
        table_name = ''

    class Field:
        _v = NumberField()

    class LSIndex:
        pass

    class GSIndex:
        pass

    _attributes: Dict[str, BaseHatsudenkiField] = {}
    _serializer: Dict[str, Callable] = {}
    _deserializer: Dict[str, Callable] = {}
    _not_scalar_key: List[str] = []
    _hook_update_key: List[BaseHatsudenkiField] = []
    _hook_put_key: List[BaseHatsudenkiField] = []
    _hash_key_name: Optional[str] = None
    _range_key_name: Optional[str] = None
    _collection_name: Optional[str] = None

    @classmethod
    def get_table_type(cls):
        if cls.Meta.is_root:
            return TableType.RootTable
        return TableType.SingleSoloTable

    @classmethod
    def _is_allow_modify(cls, key: str):
        hash_key = cls.get_hash_key_name()
        if key == hash_key:
            # ハッシュキーは変更できない
            return False
        return True

    def __init__(self, **kwargs):
        self._update_keys: Dict[str, any] = {}
        self._v = 0
        self._is_new = True

    def _modify_mark(self, key):
        self._update_keys.setdefault(key, self.__dict__.get(key, None))

    def __init_subclass__(cls, **kwargs):
        from hatsudenki.packages.manager.table import TableManager
        super().__init_subclass__(**kwargs)
        # マネージャーに登録

        if not TableManager.register(cls):
            return

        # キー情報をオーバーライド
        cls._attributes = {}
        cls._serializer = {}
        cls._deserializer = {}
        cls._not_scalar_key = []
        cls._hash_key_name = None
        cls._range_key_name = None
        cls._collection_name = None

        cls._hook_update_key = []
        cls._hook_put_key = []
        for k in dir(cls.Field):
            v = getattr(cls.Field, k)
            if isinstance(v, BaseHatsudenkiField) is False:
                # Field型ではない
                continue
            v.name = k
            cls._attributes[k] = v
            if isinstance(v, CreateDateField):
                cls._hook_put_key.append(v)
            elif isinstance(v, UpdateDateField):
                cls._hook_update_key.append(v)

            cls._serializer[k] = v.serialize
            cls._deserializer[k] = v.deserialize
            if not v.IsScalar:
                cls._not_scalar_key.append(k)

        cls._hash_key_name = cls.Meta.primary_index.hash_key
        cls._range_key_name = cls.Meta.primary_index.range_key
        cls._collection_name = cls.Meta.collection_name

        if hasattr(cls.Meta, 'alias_key_type'):
            cls.Meta.alias_key_type.name = cls.Mata.alias_key_name

    def __getitem__(self, item):
        # self[xxx]で属性にアクセスできるようにしている
        return self.__dict__[item]

    def __setattr__(self, key: str, value, force=False):
        if force or key not in self.__class__._attributes:
            # 以下の場合は何もしない
            # 強制フラグがON
            # はじめてのセット
            # DB定義されたフィールドでない
            object.__setattr__(self, key, value)
            return

        is_first_set = not hasattr(self, key)
        prev = self[key] if not is_first_set else None

        # ハッシュキーもしくはレンジキーは書き換えることが出来ない
        is_allow = self.__class__._is_allow_modify(key)
        if not is_allow and prev is not None:
            raise Exception(f'can not modify hash_key os range_key {key}')
        # 変更されたキーに追加
        if is_allow:
            self.modify_mark(key)
        super().__setattr__(key, value)

    def modify_mark(self, key):
        prev = getattr(self, str(key), None)
        self._update_keys.setdefault(key, prev)

    @property
    def hash_key_name(self):
        return self.__class__.get_hash_key_name()

    @classmethod
    def get_hash_key_name(cls):
        p = cls.get_primary_index()
        return p.hash_key

    @classmethod
    def get_collection_name(cls):
        return cls._collection_name

    @classmethod
    def get_table_name(cls):
        """
        テーブル名を取得

        :return: テーブル名文字列
        """
        return cls.Meta.table_name

    @classmethod
    def get_primary_index(cls):
        return cls.Meta.primary_index

    @classmethod
    def get_indexes(cls):
        from hatsudenki.packages.manager.table import TableManager
        return TableManager.get_index(cls.get_collection_name())

    @classmethod
    def get_field_class(cls, key):
        return cls._attributes.get(key, None)

    @classmethod
    def get_hash_field_class(cls):
        return cls.get_field_class(cls.get_hash_key_name())

    @property
    def hash_value(self):
        """
        ハッシュキーの値を取得

        :return: ハッシュキーの値（型はモデルに依存）
        """
        return getattr(self, self.hash_key_name)

    @property
    def serialized_key(self):
        """
        インスタンスのシリアライズされたハッシュキーとレンジキーを取得

        :return: シリアライズされたキー情報を格納した連想配列
        """

        c = self.__class__
        hk = c._hash_key_name

        return {
            hk: c._serializer[c._hash_key_name](self[hk])
        }

    def force_set_key(self, key, val):
        self.__setattr__(key, val, True)

    @classmethod
    def deserialize(cls: Type[T], raw_dict: dict, prj: List[str] = None) -> T:
        """
        AWSレスポンスからインスタンスを生成する

        :param raw_dict: awsアイテムレスポンス
        :return: モデルインスタンス
        """
        ret = cls()
        _attr = cls._attributes
        _set = object.__setattr__
        _der = cls._deserializer

        for key, val in raw_dict.items():
            if key in _der:
                _set(ret, key, _der[key](val, ret))

        # ユーザー操作によって更新されたものではないので更新フラグはここで一旦すべて折ってきれいにする
        # DictMap関連のappendedフラグもここで折れる
        ret.flush()
        # 新たに作られたものではない
        ret._is_new = False

        if prj is not None:
            ret._set_projection(prj)

        return ret

    def flush(self):
        """
        更新キー情報をクリア

        :return: None
        """

        d = self.__dict__

        for k in self._not_scalar_key:
            d[k].flush()

        self._update_keys = {}
        self._is_new = False

    def serialize(self):
        """
        シリアライズされたアイテム情報を取得
        :return: シリアライズされたアイテム情報を格納した連想配列
        """
        a = self.__dict__
        ret = {}
        for k, v in self.__class__._serializer.items():
            vv = v(a[k])
            if vv is not None:
                ret[k] = vv

        return ret

    @classmethod
    def not_exist_condition(cls, cond: ConditionExpression = None):
        """
        存在を確認する条件式インスタンスを生成

        :return: 存在確認式が設定された条件式インスタンス
        """
        if cond is None:
            cond = ConditionExpression()
        cond.attribute_not_exists(cls.get_hash_key_name())
        return cond

    @classmethod
    def exist_condition(cls, cond: ConditionExpression = None):
        """
        存在を確認する条件式インスタンスを生成

        :return: 存在確認式が設定された条件式インスタンス
        """
        if cond is None:
            cond = ConditionExpression()
        cond.attribute_exists(cls.get_hash_key_name())
        return cond

    def _hook_put(self):
        for pk in self.__class__._hook_put_key:
            # self.force_set_key(pk.name, TableManager.resolve_date_now())
            setattr(self, pk.name, DateManager.get_now())

    async def put(self, overwrite=False, skip_hook=False):
        """
        新規作成
        overwriteがFalse且つアイテムが存在する場合は例外が発生する

        :param overwrite: 強制上書きを行うか
        :param skip_hook: フック処理をスキップするか
        :param with_retry: リトライ処理を行うか
        :param error_level: 失敗時のエラーレベル
        :return: awsレスポンス
        """
        if not skip_hook:
            self._hook_put()
        # 強制的にカウンタを初期値に戻す
        self.force_set_key('_v', self._v + 1)
        ser = self.serialize()
        c = self.not_exist_condition() if not overwrite else None

        res = await HatsudenkiClient.put_item(
            self.get_collection_name(),
            ser,
            c
        )

        self.flush()
        return res

    def _hook_update(self):
        for pk in self.__class__._hook_update_key:
            # self.force_set_key(pk.name, TableManager.resolve_date_now())
            setattr(self, pk.name, DateManager.get_now())

    def build_update_expression(self, upd: UpdateExpression):
        for update_key, prev_val in self._update_keys.items():
            if update_key == '_v':
                # _vは後で強制的に入れるのでここでは無視
                continue
            # フィールドクラス取得
            fc = self.__class__.get_field_class(update_key)
            if fc.is_empty(self[update_key]):
                # 値が空且つ変更が行われた＝値が削除された
                upd.remove(update_key)
                continue

            # フィールドがスカラー型ではない場合は更に潜ってチェック
            now_value = self[update_key]
            fc.build_update_expression(upd, now_value)

            # upd.set(update_key, fc.serialize(self[update_key]), raw=True)

    async def update(self, upsert=False, increment=True, skip_hook=False, cond: Optional[ConditionExpression] = None):
        """
        更新
        upsertが偽且つアイテムが存在しない場合は例外が発生する
        :param upsert: レコードが存在しない場合新規作成するか
        :param increment: アトミックカウンターを考慮するか
        :param skip_hook: フック処理をスキップするか
        :param with_retry: 失敗時にリトライを行うか
        :param error_level: 失敗時のエラーレベル
        :return: awsレスポンス
        """
        if len(self._update_keys) is 0:
            # 何も更新されていないのでスキップ
            _logger.debug('update key is nothing. skip update...')
            return

        if not skip_hook:
            self._hook_update()

        # 更新されたものだけ候補に入れる
        upd = UpdateExpression()
        self.build_update_expression(upd)

        if cond is None:
            cond = ConditionExpression()
        else:
            cond.op_and()

        if not upsert:
            self.exist_condition(cond)

        if increment:
            # バージョンカウンタの操作
            upd.add('_v', 1)
            if not upsert:
                with cond:
                    cond.equal('_v', self._v)
                    cond.op_or()
                    # アイテムが存在しない時用
                    cond.attribute_not_exists('_v')

        keys = self.serialized_key
        res = await HatsudenkiClient.update_item(self.get_collection_name(), keys, upd, cond)

        self.flush()
        if increment:
            self._v += 1

        return res

    async def reget(self):
        c = await self.__class__.get(self.hash_value)
        self.copy(c)

    def copy(self, model):

        [self.force_set_key(k, model[k]) for k, v in self._attributes.items()]
        self.flush()

    @classmethod
    def get_serialized_key(cls, hash_val: any, range_val: any = None):
        """
        シリアライズされたハッシュキーとレンジキーを取得する

        :param hash_val: ハッシュキーの値（型はmodelに依存）
        :param range_val: レンジキーの値（型はmodelに依存）
        :return: シリアライズされたキー情報を格納した連想配列
        """
        cls._check_key(hash_val, range_val)

        hash_key = cls.get_hash_key_name()

        hc = cls.get_field_class(hash_key)
        ret = {
            hash_key: hc.serialize(hash_val)
        }

        return ret

    @classmethod
    def find_index(cls, hash_key: str, range_key: str = None):
        index = cls.get_indexes()
        if index.primary.check(hash_key, range_key):
            return index.primary

        if index.primary.hash_key == hash_key:
            for lsi in index.lsi:
                if lsi.check(hash_key, range_key):
                    return lsi

        for gsi in index.gsi:
            if gsi.check(hash_key, range_key):
                return gsi

        # childテーブルから参照した際に、Indexが見つからないのは正常な挙動
        # _logger.warning(f'{cls.get_table_name()} [{hash_key}, {range_key}] : INDEX NOT FOUND!!!')

        return None

    @classmethod
    async def get(cls, hash_val: any, range_val: any = None, prj_exp: List[str] = None):
        """
        ハッシュキーとレンジキー（あれば）を指定して一見取得
        :param hash_val: ハッシュキーの値
        :return: モデルインスタンス。アイテムが見つからない場合はNone
        """
        cls._check_key(hash_val, range_val)

        k = cls.get_serialized_key(hash_val, range_val)
        return await cls.get_raw(k, prj_exp)

    @classmethod
    async def get_raw(cls, raw_dict: dict, prj_exp: List[str] = None):
        r = await HatsudenkiClient.get_item(cls.get_collection_name(), raw_dict, prj_exp)
        if r is None:
            return None

        return cls.deserialize(r, prj_exp)

    @classmethod
    async def get_iter(cls: Type[T], hash_val: any, limit=0, prj_exp: List[str] = None) -> Generator[T, None, None]:
        query_dict = {
            cls.get_hash_key_name(): hash_val
        }
        return await cls.query_list(query_dict, limit, prj_exp)

    @classmethod
    def from_raw_dict_list(cls, raw_dict_list: List[dict]):
        """
        AWSレスポンスからインスタンスを生成する（配列版）

        :param raw_dict_list: awsアイテムリストレスポンス
        :return: モデルインスタンス
        """
        return map(cls.deserialize, raw_dict_list)

    @classmethod
    async def query_list(cls: Type[T],
                         query_dict: dict, limit=0, prj_exp: List[str] = None,
                         filter_dict: dict = None) -> Generator[T, None, None]:
        kc, idx = cls.query_parse(query_dict)
        index_name = idx.name if idx is not None else None
        fc = None
        if filter_dict:
            fc = cls.filter_parse(filter_dict)

        res = await HatsudenkiClient.query(table_name=cls.get_collection_name(), key_cond=kc,
                                           use_index_name=index_name, limit=limit, prj=prj_exp, filter_cond=fc)
        return cls.from_raw_dict_list(res)

    # @classmethod
    # async def paginator(cls: Type[T], query_dict: dict, page_size=100) -> Generator[T, None, None]:

    @classmethod
    async def query(cls: Type[T], query_dict: dict, prj_exp: List[str] = None, filter_dict: dict = None) -> T:
        l = await cls.query_list(query_dict, 1, prj_exp, filter_dict)
        return next(l, None)

    @classmethod
    async def query_by_cursor(cls: Type[T], cursor: str) -> T:
        return await cls.get(cursor)

    @classmethod
    async def query_list_by_cursor(cls: Type[T], cursor: str) -> List[T]:
        raise Exception('invalid operation')

    @classmethod
    async def scan_iter(cls: Type[T], filter_dict: dict = None, limit=20, prj: List[str] = None) -> \
            List[T]:
        fc = None
        if filter_dict:
            fc = cls.filter_parse(filter_dict)
        l = await HatsudenkiClient.scan(
            table_name=cls.get_collection_name(), filter_cond=fc, limit=limit, prj=prj)

        return cls.from_raw_dict_list(l)

    @classmethod
    async def scan_generator(cls: Type[T],
                             filter_dict: dict = None, tick=50, prj: List[str] = None) -> AsyncGenerator[List[T], None]:
        fc = None
        if filter_dict:
            fc = cls.filter_parse(filter_dict)
        async for l in HatsudenkiClient.scan_generator(
                table_name=cls.get_collection_name(), filter_cond=fc, tick=tick, prj=prj):
            yield cls.from_raw_dict_list(l)

    @classmethod
    async def scan_list(cls: Type[T], filter_dict: dict = None, limit=20, prj: List[str] = None) -> \
            List[T]:
        l = await cls.scan_iter(filter_dict, limit, prj)
        return list(l)

    @property
    def one_cursor(self):
        h = self.__class__.get_field_class(self.hash_key_name)
        return h.to_string(self.hash_value)

    @classmethod
    def _find_index_by_query_dict(cls, query_dict: dict):
        kv = list(query_dict.keys())
        hash_key = kv[0]
        range_key = kv[1] if len(kv) > 1 else None
        op = None

        if range_key is not None:
            sp = range_key.split('__')
            if len(sp) is 2:
                range_key = sp[0]
                op = sp[1]

        use_index = cls.find_index(hash_key, range_key)
        return hash_key, range_key, op, use_index

    @classmethod
    def query_parse(cls, query_dict: dict):
        """
        クエリ
        :param query_dict:
        :return:
        """
        hash_key, range_key, op, use_index = cls._find_index_by_query_dict(query_dict)
        if use_index is None:
            raise Exception(f'invalid keys {query_dict}')

        kc = KeyConditionExpression()

        kc.equal(hash_key, query_dict[hash_key])
        if range_key is not None:
            kc.op_and()

            if op is not None:
                rv = cls.get_serialized_field_value(range_key, query_dict[range_key + '__' + op])
                kc.get_operation_by_word(op, range_key, rv, raw=True)
            else:
                rv = cls.get_serialized_field_value(range_key, query_dict[range_key])
                kc.equal(range_key, rv, raw=True)

        return kc, use_index

    @classmethod
    def filter_parse(cls, filter_dict: dict):
        fc = FilterConditionExpression()

        for k, v in filter_dict.items():
            fc.op_and()

            fc.parse_key_value(k, v)

        return fc

    def set_hash_key(self, val):
        """
        ハッシュキーの値をセットする
        このメソッドを使わずにハッシュキーを書き換えると例外が発生する

        :param val: セットする値（型はモデルに依存）
        :return: None
        """
        hk = self.get_hash_key_name()
        fc = self.__class__.get_field_class(hk)
        self.force_set_key(hk, fc.get_data(val))

    @classmethod
    async def get_or_create(cls: Type[T], hash_val: any, range_val: any = None) -> Tuple[bool, T]:
        """
        getを行い、アイテムが存在しなければ新規インスタンスを生成して返却する。
        この際、DBへの保存も同時に行われるので注意すること。

        :param hash_val: ハッシュキーの値
        :return: [0]新規作成が行われた場合は真、[1]生成された、もしくはDBから取得されたモデルインスタンス
        """
        cls._check_key(hash_val, range_val)

        ser = cls.get_serialized_key(hash_val, range_val)
        res = await cls.get_raw(ser)
        if res is None:
            r = cls()
            r.set_hash_key(hash_val)
            if range_val:
                r.set_range_key(range_val)
            await r.put()
            return True, r
        else:
            return False, res

    @classmethod
    async def delete(cls, hash_val: any, range_val: any = None):
        cls._check_key(hash_val, range_val)

        key = cls.get_serialized_key(hash_val, range_val)
        cond = cls.exist_condition()
        await HatsudenkiClient.delete_item(cls.get_collection_name(), key, cond)

    async def remove(self):
        await self.__class__.delete(self.hash_value)

    def _set_projection(self, prj: List[str]):
        """
        指定したプロジェクション情報に応じて不要な要素の削除
        :param prj: 対象キー配列
        """
        if prj is None:
            return

        # _v は必須
        prj.append('_v')
        [self.__delattr__(key) for key in self._attributes.keys() if key not in prj]

    @classmethod
    def get_primary_key_names(cls):
        return [cls.get_hash_key_name()]

    @classmethod
    def direct_update(cls):
        from hatsudenki.packages.direct.update import DirectUpdate
        d = DirectUpdate(cls)
        return d

    def begin_update(self):
        d = self.__class__.direct_update()
        d.set_key_raw(self.serialized_key)
        self.exist_condition(d.cond)
        return d

    @classmethod
    def _check_key(cls, hash_val: any, range_val: any):
        if range_val is not None:
            raise Exception('invalid key')

    def to_dict(self):
        return {k: self[k] for k, _ in self._attributes.items()}

    async def put_or_update(self):
        if self._is_new:
            await self.put()
        else:
            await self.update()

    @classmethod
    def get_serialized_field_value(cls, field_name: str, value: any):
        fc = cls.get_field_class(field_name)
        return fc.serialize(value)

    @property
    def is_modified_record(self):
        return len(self._update_keys) > 0

    @property
    def is_created_record(self):
        return self._is_new

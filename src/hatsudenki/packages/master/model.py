import logging
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TypeVar, Type, Dict, List

import dill
import yaml

from hatsudenki.packages.cache.base.memory.static.multi import StaticCacheBaseTableMulti
from hatsudenki.packages.cache.base.memory.static.solo import StaticCacheBaseTableSolo

_logger = logging.getLogger(__name__)

T = TypeVar('T')


class MasterModelSolo(StaticCacheBaseTableSolo):
    """
    HASHキーのみを持つマスターテーブル
    """

    _cache_dict: Dict[str, Dict[any, List[T]]] = {}
    _dump_base_path: Path = None

    class Field:
        pass

    @property
    def is_nothing(self):
        """
        [なし]データか？

        :return: boolean
        """
        return self.hash_value == 'なし'

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        from hatsudenki.packages.master.manager.table import MasterTableManager
        MasterTableManager.register(cls)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def _get_by_cursor(cls: Type[T], cursor: str) -> T:
        hash_field = cls.get_hash_field_class()
        return super()._get_by_cursor(cursor)

    @classmethod
    def _load_from_yaml(cls, base_path: Path, cache_base_path: Path, force=False):
        """
        ロード処理。キャッシュが存在する場合はそちらを参照し、|
        そうでない場合はYAMLからロード後にキャッシュを生成する

        :param base_path: 検索ディレクトリパス
        :param cache_base_path: キャッシュディレクトリパス
        :param force: キャッシュを無視して強制的にYAMLを読み込む
        :return: None
        """
        cls._mapped = {}
        cls._cache_dict = {}

        yml_path = base_path / (cls.Meta.table_name + '.yml')
        cache_path = cache_base_path / (cls.Meta.table_name + '_mapped.pkl')
        _logger.info(f'load from yaml {yml_path} {cache_path}')

        # forceフラグがONの場合はキャッシュを見ない
        if not force:
            if cls._load_from_dill(cache_path):
                # キャッシュからロードに成功したのでおわり
                _logger.info(f'CACHE HIT! {cache_path} を読み込み')
                return

        # YAMLからロードし、キャッシュをダンプする
        try:
            with yml_path.open(encoding='utf-8') as file:
                ret = yaml.safe_load(file)
            _logger.info(f'{yml_path}を読み込み')
        except:
            _logger.warning(f'{yml_path}の読み込みに失敗')
            ret = []

        cls._set_records(ret)
        cls._dump_dill(cache_path)

    @classmethod
    def _load_from_dill(cls, cache_path: Path):
        """
        dillダンプからデータをロード

        :param cache_path: キャッシュパス
        :return: bool
        """

        if cache_path.exists():
            _logger.info(f'キャッシュロード {cache_path}')
            with cache_path.open(mode='rb') as f:
                cls._mapped = dill.loads(f.read())

            return True
        return False

    @classmethod
    def _dump_dill(cls, out_path: Path):
        """
        dillダンプを出力

        :param out_path: 出力先パス
        :return: None
        """
        Path(out_path.parent).mkdir(exist_ok=True, parents=True)
        with out_path.open(mode='wb') as f:
            f.write(dill.dumps(cls._mapped))

    @classmethod
    def get_dictionary(cls: Type[T], key_name: str) -> Dict[str, List[T]]:
        """
        指定したキーをハッシュとした連想配列を取得

        :param key_name: ハッシュキーとなるキー名
        :return: dict
        """

        if key_name in cls._cache_dict:
            return cls._cache_dict[key_name]

        r = {}
        for d in cls.iter():
            r.setdefault(getattr(d, key_name), [])
            r[getattr(d, key_name)].append(d)
        cls._cache_dict[key_name] = r
        return cls._cache_dict[key_name]

    @classmethod
    def find_one(cls, key_name: str, key):
        """
        キーを取得して一件取得
        この関数は指定された属性名をキーとした連想配列をキャッシュするため、一回目は重い。

        :param key_name: 対象キー名
        :param key: 検索対象の値
        :return: 条件に合致するアイテム。存在しない場合はKeyError例外をraiseする。
        :except: KeyError - みつからない
        """
        d = cls.get_dictionary(key_name)
        return d[key]

    @classmethod
    def _get(cls: Type[T], hash_key, range_key=None) -> T:
        hk = cls.get_hash_key_name()
        fc = getattr(cls.Field, hk)

        try:
            res = super()._get(fc.convert(hash_key))
        except KeyError:
            raise Exception(f'{cls.__name__} から {hash_key}:{range_key} が見つかりません')

        return res

    @classmethod
    def get_hash_field_class(cls):
        return getattr(cls.Field, cls.get_hash_key_name())

    @classmethod
    def is_multi(cls):
        return False


class MasterModelMulti(StaticCacheBaseTableMulti, MasterModelSolo):
    """
    HASHキーとRANGEキーを持つマスターテーブル
    """

    @dataclass
    class CacheData:
        mapped: dict
        sorted_table: dict

    @classmethod
    def _load_from_dill(cls, cache_path: Path):
        cls._mapped = {}
        cls._sorted_table = {}
        if cache_path.exists():
            _logger.info(f'キャッシュロード {cache_path}')
            with cache_path.open(mode='rb') as f:
                d: cls.CacheData = dill.loads(f.read())
                cls._mapped = d.mapped
                cls._sorted_table = d.sorted_table
            return True

        return False

    @classmethod
    def _dump_dill(cls, out_path: Path):
        d = cls.CacheData(mapped=cls._mapped, sorted_table=cls._sorted_table)
        Path(out_path.parent).mkdir(parents=True, exist_ok=True)
        with out_path.open(mode='wb') as f:
            f.write(dill.dumps(d))

    @classmethod
    def _get(cls: Type[T], hash_key, range_key=None) -> T:
        hk = cls.get_hash_key_name()
        fc = getattr(cls.Field, hk)
        rk = cls.get_range_key_name()
        rfc = getattr(cls.Field, rk)

        try:
            res = super()._get(fc.convert(hash_key), rfc.convert(range_key))
        except KeyError:
            raise Exception('master not found.')

        return res

    @classmethod
    def get_range_field_class(cls):
        return getattr(cls.Field, cls.get_range_key_name())

    @classmethod
    def is_multi(cls):
        return True


@dataclass
class EnumResolver:
    enum_type: Type[Enum]

    def get(self, item: str):
        return self.enum_type(int(item))


@dataclass
class MasterResolver:
    master_type: Type[MasterModelSolo]

    def get(self, item: any):
        if issubclass(self.master_type, MasterModelMulti):
            # Multiテーブルの場合はリストで返す
            return self.master_type.find(item)
        else:
            # soloテーブルの場合は単体で返す
            return self.master_type.get(item)

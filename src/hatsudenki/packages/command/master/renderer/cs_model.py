from hatsudenki.packages.command.master.table import MasterTable
from hatsudenki.packages.command.renderer.base import RenderUnit, FileRenderer
from hatsudenki.packages.command.stdout.output import IndentString


class MasterCSModelRenderUnit(RenderUnit[MasterTable]):
    """
    CSのMasterモデルファイルをレンダリングするやつ
    """

    @property
    def record_type(self):
        return f'{self.data.class_name}Record'

    @property
    def list_type(self):
        return f'List<{self.record_type}>'

    def _singleton(self):
        u = IndentString()
        # シングルトン実装
        u.add(f'private static {self.data.class_name} _instance;')
        u.add(f'public static {self.data.class_name} Instance')
        u.indent('{')
        u.add('get')
        u.indent('{')
        u.add('if (_instance == null)')
        u.indent('{')
        u.add(f'_instance = new {self.data.class_name}();')
        u.outdent('}')
        u.add('return _instance;')
        u.outdent('}')
        u.outdent('}')
        return u

    def _records(self):
        u = IndentString()
        hk = self.data.hash_key
        rk = self.data.range_key

        # レコード本体
        if rk:
            t = f'Dictionary<{hk.cs_type_name}, List<{self.record_type}>>'
        else:
            t = f'Dictionary<{hk.cs_type_name}, {self.record_type}>'
        u.add(f'private {t} _records = new {t}();')
        u.add(f'public {t} Records')
        u.indent('{')
        u.add('get')
        u.indent('{')
        u.add('return _records;')
        u.outdent('}')
        u.outdent('}')

        return u

    def _get(self):
        u = IndentString()
        hk = self.data.hash_key
        rk = self.data.range_key

        if rk:
            n = f'{hk.cs_type_name} {hk.column_name}, {rk.cs_type_name} {rk.column_name}'
            c = f'return _records[{hk.column_name}].Find(x => x.GetRangeKey() == {rk.column_name});'
        else:
            n = f'{hk.cs_type_name} {hk.column_name}'
            c = f'return _records[{hk.column_name}];'

        u.add(f'public {self.record_type} Get({n})')
        u.indent('{')
        u.add(f'{c}')
        u.outdent('}')
        return u

    def _get_list(self):
        hk = self.data.hash_key
        rk = self.data.range_key
        if rk is None:
            return None
        u = IndentString()
        n = f'{hk.cs_type_name} {hk.column_name}'
        u.add(f'public List<{self.record_type}> GetList({n})')
        u.indent('{')
        u.add(f'return _records[{hk.column_name}];')
        u.outdent('}')
        return u

    def _find(self, is_all: bool):
        if self.data.range_key is None:
            return None
        u = IndentString()
        hk = self.data.hash_key
        if is_all:
            t = f'List<{self.record_type}>'
            n = 'FindAll'
        else:
            t = self.record_type
            n = 'Find'
        u.add(f'public {t} {n}({hk.cs_type_name} {hk.column_name}, Predicate<{self.record_type}> func)')
        u.indent('{')
        u.add(f'return _records[{hk.column_name}].{n}(func);')
        u.outdent('}')
        return u

    def _filter(self, is_all: bool):
        u = IndentString()
        hk = self.data.hash_key
        if is_all:
            t = f'List<{self.record_type}>'
            n = 'FilterAll'
            fn = 'FindAll'
        else:
            t = self.record_type
            n = 'Filter'
            fn = 'Find'
        u.add(f'public {t} {n}(Predicate<{self.record_type}> func)')
        u.indent('{')
        if is_all:
            u.add(f'List<{self.record_type}> ret = new List<{self.record_type}>();')

        if self.data.range_key is None:
            u.add(f'foreach(KeyValuePair<{hk.cs_type_name}, {self.record_type}> l in _records)')
        else:
            u.add(f'foreach(KeyValuePair<{hk.cs_type_name}, List<{self.record_type}>> l in _records)')
        u.indent('{')

        if is_all:
            if self.data.range_key is None:
                u.add(f'{self.record_type} v = l.Value;')
                u.add('if(func(v))')
                u.indent('{')
                u.add('ret.Add(v);')
                u.outdent('}')
                u.outdent('}')
                u.add('return ret;')
            else:
                u.add(f'List<{self.record_type}> v = l.Value;')
                u.add(f'List<{self.record_type}> t = v.FindAll(func);')
                u.add('if(t.Count > 0)')
                u.indent('{')
                u.add('ret.AddRange(t);')
                u.outdent('}')
                u.outdent('}')
                u.add('return ret;')
        else:
            if self.data.range_key is None:
                u.add(f'{self.record_type} v = l.Value;')
                u.add('if(func(v))')
                u.indent('{')
                u.add('return v;')
                u.outdent('}')
                u.outdent('}')
                u.add('return null;')
            else:
                u.add(f'List<{self.record_type}> v = l.Value;')
                u.add(f'{self.record_type} t = v.Find(func);')
                u.add('if( t != null )')
                u.indent('{')
                u.add('return t;')
                u.outdent('}')
                u.outdent('}')
                u.add('return null;')

        u.outdent('}')

        return u

    def _load(self):
        hk = self.data.hash_key
        u = IndentString()
        u.add(f'public void FinishedLoad({self.record_type}s records)')
        u.indent('{')
        u.add('_records.Clear();')
        u.add(f'foreach({self.record_type} rec in records.Array)')
        u.indent('{')

        u.add(f'{hk.cs_type_name} hk = rec.GetHashKey();')

        if self.data.range_key is None:
            u.add('_records.Add(hk, rec);')
        else:
            u.add('if(!_records.ContainsKey(hk))')
            u.indent('{')
            u.add(f'_records.Add(hk, new List<{self.record_type}>());')
            u.outdent('}')
            u.add('_records[hk].Add(rec);')

        u.outdent('}')
        u.outdent('}')
        return u

    def _repository(self):
        u = IndentString()
        u.add(f'public class {self.data.class_name}')
        u.indent('{')
        u.add(f'public const string TableName = "{self.data.table_name}";')
        # シングルトン実装
        u.add(self._singleton())
        # コンストラクタ
        u.add(f'private {self.data.class_name}() {{ }}')
        # レコード本体
        u.add(self._records())

        # 一件取得
        u.add(self._get())
        u.add(self._get_list())

        # find
        u.add(self._find(is_all=False))
        u.add(self._find(is_all=True))
        # filter
        u.add(self._filter(is_all=False))
        u.add(self._filter(is_all=True))

        # loader
        u.add(self._load())

        u.add(f'public void Release()')
        u.indent('{')
        u.add('_records.Clear();')
        u.add('_records = null;')
        u.add('_instance = null;')
        u.outdent('}')

        u.outdent('}')

        return u

    def _store(self):
        u = IndentString()
        u.add('[MessagePackObject]')
        u.add(f'public class {self.record_type}s')
        u.indent('{')
        u.add('[Key(0)]')
        u.add(f'public {self.record_type}[] Array;')
        u.outdent('}')
        return u

    def _model(self):
        u = IndentString()
        u.add('[MessagePackObject]')
        u.add(f'public class {self.record_type}')
        u.indent('{')

        keys = [c for c in self.data.columns.values() if not c.is_no_pack]
        keys.extend([c for c in self.data.shadow_columns.values() if not c.is_no_pack])

        args = ', '.join((c.cs_argument_name for c in keys))

        u.add(f'public {self.record_type}({args})')
        u.indent('{')
        [u.add(c.cs_assign_name) for c in keys]
        u.outdent('}')

        for idx, c in enumerate(keys):
            if c.is_no_pack:
                continue
            u.add(f'[Key({idx})]', c.cs_define_name)

        hk = self.data.hash_key
        u.add(f'public {hk.cs_type_name} GetHashKey()')
        u.indent('{')
        u.add(f'return {hk.cs_name};')
        u.outdent('}')

        rk = self.data.range_key
        if rk:
            u.add(f'public {rk.cs_type_name} GetRangeKey()')
            u.indent('{')
            u.add(f'return {rk.cs_name};')
            u.outdent('}')

        for c in keys:
            if not c.is_no_pack:
                u.add(c.cs_access_name())

        u.outdent('}')

        return u

    def render(self):
        r = self._repository()
        r.add(self._store())
        r.add(self._model())

        return r


class MasterCSModelRenderer(FileRenderer):

    def render_header(self):
        u = IndentString()
        u.add('using System;')
        u.add('using System.Collections.Generic;')
        u.add('using MessagePack;')
        return u

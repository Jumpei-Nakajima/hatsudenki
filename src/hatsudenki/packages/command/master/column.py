import datetime
from typing import Optional, Dict

from hatsudenki.packages.command.stdout.output import ToolOutput, snake_to_camel, IndentString


def master_column(type_str: str, python_type_str: str, cs_type_str: str, bq_type_str: str):
    """
    Masterのカラムであることを表す
    :param type_str: yaml上で設定するべきtype文字列
    :param python_type_str: python上で扱われるtype文字列
    :param cs_type_str: cs上で扱われるtype文字列
    :param bq_type_str: BigQuery上で扱われるtype文字列
    :return:
    """

    def deco(func):
        func.type_name = type_str
        func.python_type_name = python_type_str
        func._cs_type_name = cs_type_str
        func._bq_type_name = bq_type_str

        ColumnFactory.register(type_str, func)
        return func

    return deco


class ColumnFactory(object):
    """
    typeの値を元に対応したColumnインスタンスを生成する
    """
    _type_table = {}

    @classmethod
    def create(cls, table_name: str, key_name: str, data: dict, master_loader):
        try:
            type = data['type']
            return cls._type_table[type](table_name, key_name, data, master_loader)
        except Exception as e:
            ToolOutput.out_error(f'columnの生成に失敗 table={table_name}, key={key_name}')
            raise e

    @classmethod
    def register(cls, type_name, column_cls):
        cls._type_table[type_name] = column_cls


class MasterColumn(object):
    """
    Masterカラムのベース
    """

    type_name: str = ''
    python_type_name: str = ''
    _cs_type_name: str = ''
    _bq_type_name: str = ''

    def __init__(self, table_name: str, key_name: str, data: dict, master_loader):
        from hatsudenki.packages.command.master.enum.loader import EnumLoader
        from hatsudenki.packages.command.master.loader import MasterTableLoader
        self.table_name = table_name
        self.column_name = key_name
        self.data = data
        self.master_loader: MasterTableLoader = master_loader
        self.enum_loader: EnumLoader = self.master_loader.enum_loader

    @property
    def cs_type_name(self):
        return self.__class__._cs_type_name

    @property
    def bq_type_name(self):
        return self.__class__._bq_type_name

    @property
    def is_relation(self):
        return False

    @property
    def excel_header_name(self):
        return self.data.get('name', self.column_name)

    @property
    def excel_raw_header_name(self):
        return self.data.get('name', self.column_name)

    def get_link_label(self):
        return None

    @property
    def table(self):
        return self.master_loader.get_by_table_name(self.table_name)

    @property
    def python_name(self):
        return self.column_name

    @property
    def python_define_name(self):
        return f'{self.python_name} = column.{self.python_type_name}(name="{self.python_name}")'

    @property
    def python_init_name(self):
        return f"self.{self.python_name} = fields.{self.python_name}.convert(kwargs.get(fields.{self.python_name}.name))"

    @property
    def cs_name(self):
        """
        CS側の変数名

        :return: str
        """
        return snake_to_camel(self.python_name)

    @property
    def cs_argument_name(self):
        """
        CS側の引数名

        :return: str
        """
        return f'{self.cs_type_name} {self.column_name}'

    @property
    def cs_assign_name(self):
        return f'{self.cs_name} = {self.column_name};'

    @property
    def cs_define_name(self):
        return f'public {self.cs_type_name} {self.cs_name} {{ get; set; }}'

    @property
    def is_hash_key(self):
        return self.data.get('hash', False)

    @property
    def is_range_key(self):
        return self.data.get('range', False)

    @property
    def is_serial_key(self):
        return self.data.get('is_serial', False)

    @property
    def is_no_pack(self):
        if not self.table.is_out_pack:
            # テーブル自体がPackをスキップする設定の場合は無条件にスキップ
            return True
        return self.data.get('no_pack', False)

    @property
    def comment(self):
        return self.data.get('comment', None)

    def generate_value(self, value: any):
        return value

    def reverse_value(self, value: any):
        return value

    def cs_access_name(self):
        return None

    @property
    def value_limit(self) -> Optional[Dict[str, any]]:
        return None

    @property
    def serial_resolver_name(self):
        if not self.is_serial_key:
            return None

        serial = f"'{self.table_name}_'" + f' + str(self.{self.column_name}).zfill(10)'

        i = IndentString()

        i.add('@property')
        i.indent('def log_id(self):')
        i.add(f'return {serial}')

        return i

@master_column('number', 'MasterColumnInt', 'uint', 'INTEGER')
class ColumnNumber(MasterColumn):
    def generate_value(self, value: any):
        info: str = f'({self.table.excel_name}.xlsx, sheet: {self.table.excel_sheet_name}, column: {self.excel_header_name})'
        if type(value) == str:
            try:
                return int(value)
            except ValueError as e:
                raise Exception(f'数値カラムに文字列が入っている({info})')

        if value < 0:
            raise Exception(f'uint数値カラムに負数が入っている({info})')
        if type(value) != int:
            raise Exception(f'数値カラムに整数ではない値が入っている。小数？({info})')
        return value


@master_column('string', 'MasterColumnString', 'string', 'STRING')
class ColumnString(MasterColumn):
    def generate_value(self, value: any):
        if value is not None:
            return str(value)

        return ''


@master_column('master', 'MasterColumnRelation', 'string', 'STRING')
class ColumnRelation(MasterColumn):

    @property
    def cs_type_name(self):
        target = self.get_target_table()
        if target.range_key is not None:
            return 'string'
        else:
            return target.hash_key.cs_type_name

    @property
    def bq_type_name(self):
        target = self.get_target_table()
        if target.range_key is not None:
            return 'string'
        else:
            return target.hash_key.bq_type_name

    @property
    def is_relation(self):
        return True

    def get_link_label(self):
        to = self.to
        self_table = self.table
        target_table = self.master_loader.get_by_table_name(to)
        if self_table.excel_name == target_table.excel_name:
            return f'#{target_table.excel_sheet_name}!A1'
        else:
            return f'{target_table.excel_name}.xlsx#{target_table.excel_sheet_name}!A1'

    @property
    def to(self):
        return self.data['to']

    def get_target_table(self):
        return self.master_loader.get_by_table_name(self.to)

    @property
    def python_define_name(self):
        return f'{self.python_name} = column.{self.python_type_name}(name="{self.python_name}", to="{self.data["to"]}")'

    @property
    def resolver_name(self):
        u = IndentString()
        u.add('@property')
        u.indent(f'def resolved_{self.python_name}(self):')

        t = self.get_target_table()
        if t.range_key is None:
            u.add(f'a = {self.get_target_table().class_name}.get(self.{self.python_name})')
            u.indent('if a is None:')
            u.add('return None')
            u.outdent()
            u.indent('if a.is_nothing:')
            u.add('return None')
            u.outdent()
            u.add('return a')
        else:
            u.add(f'a = {self.get_target_table().class_name}.find(self.{self.python_name})')
            u.indent('if len(a) > 0 and a[0].is_nothing:')
            u.add('return None')
            u.outdent()
            u.add('return a')
        return u

    def cs_access_name(self):
        u = IndentString()
        t = self.get_target_table()

        u.add(f'public {t.class_name} {self.cs_name}Table()')
        u.indent('{')
        u.add(f'return {t.class_name}.Instance;')
        u.outdent('}')
        return u

    @property
    def value_limit(self) -> Optional[Dict[str, any]]:
        t = self.get_target_table()
        return {
            'master': t
        }


@master_column('enum', 'MasterColumnEnum', 'uint', 'INTEGER')
class ColumnEnum(MasterColumn):
    @property
    def is_relation(self):
        return True

    def get_link_label(self):
        to = self.data['to']
        target_table = self.enum_loader.get_by_name(to)
        return f'pg_excel/定義タイプ.xlsx#{target_table.excel_sheet_name}!A1'

    @property
    def to(self):
        return self.data['to']

    def resolve_relation(self):
        return self.enum_loader.get_by_name(self.to)

    def reverse_value(self, value: any):
        en = self.resolve_relation()
        v = en.find(value, is_raw=True)
        if v is None:
            l = f'{self.table.excel_name}.xlsx {self.excel_raw_header_name}'
            raise Exception(f'{self.table_name}: {en.excel_sheet_name} から {value} が見つかりません\n{l}')
        return v.excel_name

    def generate_value(self, value):
        en = self.resolve_relation()
        excel = en.find(value, is_raw=False)
        if excel is None:
            l = f'{self.table.excel_name}.xlsx {self.table.excel_sheet_name} {self.excel_raw_header_name}'
            raise Exception(f'enum not found {self.to} {value} {l}')
        return excel.id

    @property
    def value_limit(self):
        en = self.resolve_relation()
        return {
            'choice': en.values
        }


@master_column('date', 'MasterColumnDate', 'uint', 'TIMESTAMP')
class ColumnDatetime(MasterColumn):
    def generate_value(self, value: any):
        if type(value) is datetime.datetime:
            return int(value.timestamp())
        if value == None:
            return 0
        t = f'{self.table.excel_name}.xlsx {self.table.excel_sheet_name} {self.excel_raw_header_name}'
        raise Exception(f'invalid datetime {value} {t}')

    def reverse_value(self, value: any):
        return datetime.datetime.fromtimestamp(value)


@master_column('select', 'MasterColumnSelect', 'uint', 'INTEGER')
class ColumnSelect(ColumnEnum):
    @property
    def label(self):
        return f'[Select] {self.table.label}:{self.excel_raw_header_name}({self.column_name})'

    def resolve_target_name(self, val, is_raw=False) -> str:
        enum_data = self.resolve_relation()
        v = enum_data.find(val, is_raw=is_raw)
        if v is None:
            raise Exception(f'{self.label} から {val} が見つかりません is_raw={is_raw}')
        return v.data['value']

    def resolve_target(self, val, is_raw=False):
        n = self.resolve_target_name(val, is_raw)
        if n.startswith('enum_'):
            # enum_から始まっているターゲットはenumとして扱う
            return self.enum_loader.get_by_name(n)
        else:
            return self.master_loader.get_by_table_name(n)


@master_column('chose', 'MasterColumnChose', 'string', 'STRING')
class ColumnChose(MasterColumn):
    @property
    def selector(self) -> str:
        return f'{self.data["selector"]}'

    @property
    def label(self):
        return f'[Chose] {self.table.label}:{self.excel_raw_header_name}({self.column_name})'

    @property
    def selector_column(self) -> ColumnSelect:
        sc = self.table.columns.get(self.selector, None)
        if sc is None:
            raise Exception(f'ChoseColumnの解決に失敗 {self.table.label} から {self.selector} カラム が見つかりません')
        if isinstance(sc, ColumnSelect):
            return sc
        else:
            raise Exception(f'ChoseColumnの解決に失敗 {self.table.label} の {self.selector} カラム は Selectorではありません')

    def _resolve_value(self, s, val, is_raw=False, ret_ref_name=False):
        from hatsudenki.packages.command.master.enum.data import EnumData
        from hatsudenki.packages.command.master.table import MasterTable
        if isinstance(s, EnumData):
            en_data = s.find(val, is_raw)
            if en_data is None:
                raise Exception(f'{self.label} の解決に失敗 {s.label} から {val} が見つかりません')
            if ret_ref_name:
                return en_data.excel_name
            else:
                return en_data.id
        elif isinstance(s, MasterTable):
            return val
        else:
            raise Exception(f'{self.label} selectorの戻り値が不正です')

    def resolver_name(self, selector: ColumnSelect):
        u = IndentString()
        te = selector.resolve_relation()

        u.add('@property')
        u.indent(f'def resolved_{self.python_name}(self):')
        u.add('from common.enums import def_enum')
        u.add(f'tbl = def_enum.Enum{te.full_classname}.resolve(self.{selector.python_name})')
        u.add(f'return tbl.get(self.{self.python_name})')
        return u

    def resolve_target(self, val, is_raw=False):
        sc = self.selector_column
        return sc.resolve_target(val, is_raw)

    def resolve_value(self, sel, value, is_raw=False, ret_ref_name=False):
        t = self.resolve_target(sel, is_raw)
        return self._resolve_value(t, value, is_raw, ret_ref_name)

    def generate_value(self, value: any):
        return value

    def reverse_value(self, value: any):
        raise Exception('ChoseColumnはSelectorの値が無いと確定しません')

from hatsudenki.define.config import TAG_SEPARATOR
from hatsudenki.packages.command.stdout.output import IndentString


class HatsudenkiFieldBase(object):
    """
    Fieldベース
    """
    # YAMLにて定義される値
    TypeStr = ''
    # アプリ側でのフィールドクラス名
    PythonStr = ''
    # アプリ側でのバリュークラス名
    PythonValueTypeStr = ''
    # 個別にクラスを生成する必要があるか
    HasGenClass = False

    def __init__(self, name: str, raw_dict: dict, parent):
        """
        イニシャライザ

        :param name: 名前
        :param raw_dict: yamlデータ
        :param parent: 親オブジェクト
        """
        from hatsudenki.packages.command.hatsudenki.data import HatsudenkiData
        self.name = name
        self.data = raw_dict
        self.parent: HatsudenkiData = parent

    @property
    def default_value(self):
        """
        デフォルト値

        :return: any
        """
        return self.data.get('default', None)

    @property
    def python_field_type_str(self):
        """
        Pythonフィールドクラス名
        :return: str
        """
        return self.__class__.PythonStr

    @property
    def python_value_type_str(self):
        """
        pythonバリュータイプ名

        :return: str
        """
        return self.__class__.PythonValueTypeStr

    @property
    def python_init_type_str(self):
        """
        イニシャライザ初期化パラメータとして受け取る型名
        :return: str
        """
        # 基本的にValueTypeと同一
        return self.__class__.PythonValueTypeStr

    def _parse_opt(self):
        """
        追加のオプション

        :return: list
        """
        # デフォルトではなにもない
        opt = []
        return opt

    def get_init_option(self, extra: dict = None):
        """
        イニシャライザのオプション
        :param extra: 追加オプション
        :return: str
        """
        # 型独自の追加オプションを処理
        opt = self._parse_opt()

        # デフォルト値があれば追加する
        d = self.default_value
        if d is not None:
            opt.append(f'default={d}')

        # 追加オプションがあれば追加する
        if extra is not None:
            for k, v in extra.items():
                opt.append(f'{k}={str(v)}')
        return ', '.join(opt)

    def gen_init_str(self, direct_assign=True):
        """
        イニシャライザ内での初期化処理

        :return: str
        """
        if direct_assign:
            if self.is_alias_key:
                return f'self.{self.name} = self.Meta.key_alias_type.get_data({self.name}, self)'
            elif self.is_hash or self.is_range:
                return f'self.{self.name}: {self.python_value_type_str} = ft.{self.name}.get_data({self.name}, self)'
        return f'self.{self.name}: {self.python_value_type_str} = ft.{self.name}.get_data_from_dict(kwargs, self)'

    def gen_def_str(self, ex_opt: dict = None):
        """
        Fieldクラス内での定義処理

        :param ex_opt: 追加オプション
        :return: str
        """
        return f"{self.name} = {self.def_python_field_class(ex_opt)}"

    def def_python_field_class(self, extra_opt: dict = None):
        """
        Fieldクラスの定義文字列

        :param extra_opt: 追加オプション
        :return: str
        """
        return f'{self.python_field_type_str}({self.get_init_option(extra_opt)})'

    @property
    def is_alias_key(self):
        """
        エイリアスキーか

        :return: boolean
        """
        return self.is_range and not self.parent.is_alone

    @property
    def is_hash(self):
        """
        ハッシュキーか

        :return: boolean
        """
        return self.data.get('hash', False)

    @property
    def is_range(self):
        """
        レンジキーか

        :return: boolean
        """
        return self.data.get('range', False)

    @property
    def resolver_string(self):
        """
        リゾルバ

        :return: IndexString
        """
        if not self.is_alias_key:
            # エイリアスキーでなければ不要
            return None

        # エイリアスキーはリゾルバが必要
        l = len(self.parent.tag_name) + len(TAG_SEPARATOR)
        parent = self.parent.parent_table
        body = IndentString()
        # getter
        body.add('@property')
        body.indent(f'def {self.name}(self) -> {self.PythonValueTypeStr}:')
        body.add(f'return {self.PythonValueTypeStr}(self.{parent.range_key.name}[{l}:])')
        body.outdent()

        body.blank_line()

        # setter
        body.add(f'@{self.name}.setter')
        body.indent(f'def {self.name}(self, val: {self.PythonValueTypeStr}):')
        body.indent('if val is None:')
        body.add('return')
        body.outdent()
        body.add(
            f'self.{self.parent.parent_table.range_key.name} = f"{self.parent.tag_name}{TAG_SEPARATOR}{self.resolver_setter_name}"')
        body.outdent()

        return body

    @property
    def resolver_setter_name(self):
        return '{val}'

    @property
    def class_string(self):
        return None

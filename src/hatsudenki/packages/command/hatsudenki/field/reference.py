from hatsudenki.define.config import TAG_SEPARATOR
from hatsudenki.packages.command.hatsudenki.field import field, HatsudenkiFieldBase
from hatsudenki.packages.command.stdout.output import snake_to_camel, IndentString


@field('dynamo_one', 'str')
class ReferenceDynamoOneField(HatsudenkiFieldBase):
    PythonStr = 'field.ReferenceDynamoOneField'

    def _parse_opt(self):
        o = super()._parse_opt()
        o.append(f"to='{self.target_table_name}'")
        return o

    @property
    def field_type(self):
        return f'field.ReferenceDynamoOneField'

    @property
    def python_value_type_str(self):
        return f'{self.field_type}.Value[{self.target_table_class_name}]'

    def def_python_field_class(self, extra_opt: dict = None):
        return f'{self.field_type}({self.get_init_option(extra_opt)})'

    @property
    def target_table_name(self) -> str:
        return self.data['to']

    @property
    def target_table_class_name(self):
        return snake_to_camel(self.target_table_name)

    @property
    def resolver_string(self):
        if not self.is_alias_key:
            return None
        if self.is_range:
            # 子テーブル且つレンジキーの場合はエイリアスキーとなる
            l = len(self.parent.tag_name) + len(TAG_SEPARATOR)
            parent = self.parent.parent_table
            parent_rk_name = parent.range_key.name

            body = IndentString()

            # getter
            body.add('@property')
            body.indent(f'def {self.name}(self) -> {self.python_value_type_str}:')
            body.indent(f'if self.{parent_rk_name}:')
            body.add(f'd = self.Meta.key_alias_type.get_data(self.{parent_rk_name}[{l}:])')
            body.outdent()
            body.indent('else:')
            body.add('d = self.Meta.key_alias_type.get_data(None)')
            body.outdent()
            body.add('d.parent = self')
            body.add('return d')
            body.outdent()

            body.blank_line()

            # setter
            body.add(f'@{self.name}.setter')
            body.indent(f'def {self.name}(self, val):')
            body.indent('if val is None:')
            body.add('return')
            body.outdent()
            body.indent('if val.is_empty():')
            body.add('return')
            body.outdent()
            body.add(
                f'self.{parent_rk_name} = f"{self.parent.tag_name}{TAG_SEPARATOR}{{val}}"')
            body.outdent()

            return body
        return None


@field('dynamo_many', 'str')
class ReferenceDynamoManyField(ReferenceDynamoOneField):
    PythonStr = 'field.ReferenceDynamoManyField'

    @property
    def field_type(self):
        return f'field.ReferenceDynamoManyField'


@field('master_one', 'str')
class ReferenceMasterOneField(HatsudenkiFieldBase):
    PythonStr = 'field.ReferenceMasterOneField'

    def gen_init_str(self, direct_assign=True):
        if direct_assign:
            if self.is_alias_key:
                return f'self.{self.name} = self.Meta.key_alias_type.get_data({self.name}, self)'
            elif self.is_hash or self.is_range:
                return f'self.{self.name} = ft.{self.name}.get_data({self.name}, self)'
        return f'self.{self.name} = ft.{self.name}.get_data_from_dict(kwargs, self)'

    def _parse_opt(self):
        o = super()._parse_opt()
        o.append(f"to=masters.{self.target_table_class_name}")
        return o

    @property
    def field_type(self):
        return f'field.ReferenceMasterOneField'

    @property
    def python_init_type_str(self):
        return 'masters.' + self.target_table_class_name

    @property
    def python_value_type_str(self):
        return f'{self.field_type}.Value'

    def def_python_field_class(self, extra_opt: dict = None):
        return f'{self.field_type}({self.get_init_option(extra_opt)})'

    @property
    def target_table_name(self) -> str:
        return self.data['to']

    @property
    def target_table_class_name(self):
        return 'Master' + snake_to_camel(self.target_table_name)

    @property
    def resolver_string(self):
        if not self.is_alias_key:
            return None
        if self.is_range:
            # 子テーブル且つレンジキーの場合はエイリアスキーとなる
            l = len(self.parent.tag_name) + len(TAG_SEPARATOR)
            parent = self.parent.parent_table
            parent_rk_name = parent.range_key.name

            body = IndentString()

            # getter
            body.add('@property')
            body.indent(f'def {self.name}(self):')
            body.indent(f'if self.{parent_rk_name}:')
            body.add(f'd = self.Meta.key_alias_type.get_data(self.{parent_rk_name}[{l}:])')
            body.outdent()
            body.indent('else:')
            body.add('d = self.Meta.key_alias_type.get_data(None)')
            body.outdent()
            body.add('d.parent = self')
            body.add('return d')
            body.outdent()

            body.blank_line()

            # setter
            body.add(f'@{self.name}.setter')
            body.indent(f'def {self.name}(self, val):')
            body.indent('if val is None:')
            body.add('return')
            body.outdent()
            body.indent('if val.is_empty():')
            body.add('return')
            body.outdent()
            body.add(
                f'self.{parent_rk_name} = f"{self.parent.tag_name}{TAG_SEPARATOR}{{val}}"')
            body.outdent()

            return body
        return None


@field('master_many', 'str')
class ReferenceMasterManyField(ReferenceMasterOneField):
    PythonStr = 'field.ReferenceMasterManyField'

    @property
    def field_type(self):
        return f'field.ReferenceMasterManyField'

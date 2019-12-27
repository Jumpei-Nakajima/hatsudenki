from hatsudenki.define.config import TAG_SEPARATOR
from hatsudenki.packages.command.hatsudenki.field import HatsudenkiFieldBase, field
from hatsudenki.packages.command.stdout.output import snake_to_camel, IndentString


@field('keys', 'str')
class MasterKeysColumn(HatsudenkiFieldBase):
    PythonStr = 'field.MasterKeysField'
    # keysは個別のクラス生成が必要
    HasGenClass = True

    def get_target_table_class_name(self, idx: int):
        t = self.data['targets'][idx]
        if self.is_master(t):
            return 'masters.' + snake_to_camel(t)
        if self.is_enum(t):
            return 'def_enum.' + snake_to_camel(t)
        return snake_to_camel(t)

    def is_master(self, key: str):
        return key.startswith('master_')

    def is_enum(self, key: str):
        return key.startswith('enum_')

    @property
    def is_alias_key(self):
        return self.is_range and not self.parent.is_alone

    @property
    def targets(self):
        return self.data['targets']

    def def_python_field_class(self, extra_opt: dict = None):
        targets = self.data['targets']
        length = len(targets)
        return f'{snake_to_camel(self.name)}Field(name="{self.name}", split_num={length})'

    @property
    def python_value_type_str(self):
        if self.is_alias_key:
            return f'{self.parent.class_name}.Meta.{snake_to_camel(self.name)}Field.Value'
        else:
            return f'{self.parent.class_name}.Field.{snake_to_camel(self.name)}Field.Value'

    @property
    def python_init_type_str(self):
        return 'list'

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

    @property
    def class_string(self):
        u = IndentString(f'class {snake_to_camel(self.name)}Field(field.MasterKeysField):')

        u.indent('class Value(field.MasterKeysField.Value):')

        acce = IndentString()
        excep = IndentString()
        opt_strings = []

        con_body = IndentString()
        for idx, t in enumerate(self.targets):
            acce.add('@property')
            acce.indent(f'def t{idx}(self):')
            c = self.get_target_table_class_name(idx)

            if self.is_enum(t):
                acce.add(f'return {c}(int(self.label_list[{idx}])) if self.label_list[{idx}] else None')
            else:
                acce.add(f'return self.label_list[{idx}]')
            acce.outdent('')

            if self.is_master(t):
                acce.indent(f'def resolved_t{idx}(self):')
                acce.add(f'return {c}.get_by_cursor(self.t{idx})')
                acce.outdent('')
            elif self.is_enum(t):
                # enumにリゾルバは存在しない
                pass
            else:
                acce.indent(f'async def resolved_t{idx}(self):')
                acce.add(f'return await {c}.query_by_cursor(self.t{idx})')
                acce.outdent('')

            excep.indent(f'if not isinstance(t{idx}, {c}):')
            excep.add(f'raise Exception(f"not [{c}] given {{type(t{idx}).__name__}}")')
            excep.outdent()
            opt_strings.append(f't{idx}: {c}')

            if self.is_enum(t):
                con_body.add(f'self.label_list[{idx}] = str(t{idx}.value)')
            else:
                con_body.add(f'self.label_list[{idx}] = t{idx}.one_cursor')

        if not self.parent.is_alone:
            con_body.add('self.update_key()')

        con_head = IndentString(f'def connect(self, {", ".join(opt_strings)}):')
        con_head.add(excep)
        con_head.add(con_body)

        u.add(acce, con_head)

        return u

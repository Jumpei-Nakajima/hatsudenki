from hatsudenki.define.config import TAG_SEPARATOR
from hatsudenki.packages.command.hatsudenki.field.util import field
from hatsudenki.packages.command.hatsudenki.field.base import HatsudenkiFieldBase
from hatsudenki.packages.command.stdout.output import snake_to_camel, IndentString


@field('uuid', 'uuid.UUID')
class UUIDField(HatsudenkiFieldBase):
    PythonStr = 'field.UUIDField'

    @property
    def resolver_setter_name(self):
        return '{val.hex}'


@field('date', 'datetime')
class DateField(HatsudenkiFieldBase):
    PythonStr = 'field.DateField'

    def _parse_opt(self):
        o = super()._parse_opt()
        t = self.data.get('ttl', None)
        if t is not None and t is True:
            o.append(f'ttl=True')
        return o


@field('update_date', 'datetime')
class UpdateDateField(HatsudenkiFieldBase):
    PythonStr = 'field.UpdateDateField'


@field('create_date', 'datetime')
class CreateDateField(HatsudenkiFieldBase):
    PythonStr = 'field.CreateDateField'


@field('enum', 'int')
class EnumField(HatsudenkiFieldBase):
    PythonStr = 'field.EnumField'

    def gen_init_str(self, direct_assign=True):
        return super().gen_init_str(direct_assign)

    def _parse_opt(self):
        o = super()._parse_opt()
        o.append(f"to=def_enum.{self.target_enum_name}")
        return o

    @property
    def target_enum_name(self):
        return f"Enum{snake_to_camel(self.data['to'])}"

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
            body.indent(f'def {self.name}(self) -> def_enum.{self.target_enum_name}:')
            body.add(f'return self.Meta.key_alias_type.to(int(self.{parent_rk_name}[{l}:]))')
            body.outdent()
            body.blank_line()
            # setter
            body.add(f'@{self.name}.setter')
            body.indent(f'def {self.name}(self, val):')
            body.indent('if val is None:')
            body.add('return')
            body.outdent()
            body.add(f'self.{parent_rk_name} = f"{self.parent.tag_name}{TAG_SEPARATOR}{{val.value}}"')
            body.outdent()
            body.outdent()

            return body
        return None

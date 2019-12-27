from typing import Dict

from hatsudenki.packages.command.hatsudenki.field.base import HatsudenkiFieldBase
from hatsudenki.packages.command.hatsudenki.field.util import FieldFactory, field
from hatsudenki.packages.command.stdout.output import IndentString, snake_to_camel


class ChildObject(object):
    def __init__(self, data: dict):
        self.data = data
        # ファクトリを通してタイプに応じたフィールドクラスをインスタンス化した後に格納する
        self.attributes: Dict[str, HatsudenkiFieldBase] = {k: FieldFactory.create_field(k, v, self) for k, v in
                                                           self.data['attributes'].items()}

    def render(self, name):
        """
        クラス出力

        :param name: フィールド名
        :return: IndentString
        """
        u = IndentString()
        u.indent(f'class {name}(MarkedObject):')
        u.indent('class Field:')
        for k, v in self.attributes.items():
            u.add(v.class_string)
            u.add(v.gen_def_str())

        u.outdent()
        u.blank_line()
        u.indent('def __init__(self, name, parent: Markable, **kwargs):')
        u.add('super().__init__(name, parent)')
        u.add('ft = self.__class__.Field')
        for k, v in self.attributes.items():
            u.add(v.gen_init_str(direct_assign=False))

        u.add('')
        return u


class IndexedChildObject(ChildObject):

    def render(self, name):
        hash_key = next((v for k, v in self.attributes.items() if v.is_hash))

        u = IndentString()
        u.indent(f'class {name}(MarkedObjectWithIndex):')
        u.indent('class Meta:')
        u.add(f"hash_key = '{hash_key.name}'")
        u.outdent()
        u.blank_line()
        u.indent('class Field:')
        for k, v in self.attributes.items():
            u.add(v.class_string)
            u.add(v.gen_def_str())

        u.outdent()
        u.blank_line()
        u.indent('def __init__(self, name, parent: Markable, **kwargs):')
        u.add('super().__init__(name, parent)')
        u.add('ft = self.__class__.Field')
        for k, v in self.attributes.items():
            u.add(v.gen_init_str(direct_assign=False))

        u.add('')
        return u


@field('list', 'list')
class ListField(HatsudenkiFieldBase):
    PythonStr = 'field.ListField'

    @property
    def child(self):
        """
        小要素の定義

        :return: ChildObjectインスタンス
        """
        v = self.data.get('value')
        return ChildObject(v)

    def _parse_opt(self):
        f = super()._parse_opt()
        f.append(f'value_type={self.value_class_name}')
        return f

    @property
    def value_class_name(self):
        """
        小要素のタイプネーム

        :return: str
        """
        return snake_to_camel(self.name) + 'Value'

    @property
    def class_string(self):
        c = self.child
        return c.render(self.value_class_name)

    def gen_def_str(self, ex_opt: dict = None):
        return f"{self.name}: {self.PythonStr}[{self.value_class_name}] = {self.def_python_field_class(ex_opt)}"

    def gen_init_str(self, direct_assign=True):
        return f'self.{self.name} = ft.{self.name}.get_data_from_dict(kwargs, self)'


@field('dict', 'dict')
class DictField(HatsudenkiFieldBase):
    PythonStr = 'field.DictField'


@field('map', 'dict')
class MapField(HatsudenkiFieldBase):
    PythonStr = 'field.MapField'

    def _parse_opt(self):
        f = super()._parse_opt()
        f.append(f'value_type={self.value_class_name}')
        return f

    @property
    def value_class_name(self):
        """
        小要素のタイプネーム

        :return: str
        """
        return snake_to_camel(self.name) + 'Value'

    @property
    def class_string(self):
        c = self.child
        return c.render(self.value_class_name)

    @property
    def child(self):
        """
        小要素の定義

        :return: ChildObjectインスタンス
        """
        v = self.data
        return ChildObject(v)

    def gen_def_str(self, ex_opt: dict = None):
        return f"{self.name}: {self.PythonStr}[{self.value_class_name}] = {self.def_python_field_class(ex_opt)}"

    def gen_init_str(self, direct_assign=True):
        return f'self.{self.name} = ft.{self.name}.get_data_from_dict(kwargs, self)'


@field('dict_map', 'dict')
class DictMapField(ListField):
    PythonStr = 'field.DictMapField'

    @property
    def child(self):
        return IndexedChildObject(self.data.get('value'))

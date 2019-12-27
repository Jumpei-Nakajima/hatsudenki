from hatsudenki.packages.command.master.enum.data import EnumData
from hatsudenki.packages.command.renderer.base import RenderUnit, FileRenderer
from hatsudenki.packages.command.stdout.output import IndentString


class PythonEnumRenderUnit(RenderUnit[EnumData]):

    def _enum(self):
        u = IndentString()
        u.indent(f'class Enum{self.data.full_classname}(enum.IntEnum):')
        for v in self.data.values:
            u.add(v.python_str)

        if self.data.is_selector:
            u.blank_line()
            u.add('@classmethod')
            u.indent('def resolve(cls, value: int):')
            u.add('from common import masters')
            for v in self.data.values:
                u.add(v.resolve_name)
            u.outdent()

        # 日本語名を出力します
        u.blank_line()
        u.add('@classmethod')
        u.indent('def reverse(cls, value: int) -> str:')
        for v in self.data.values:
            u.add(f'if value == cls.{v.enum_value_name}: return "{v.excel_name}"')
        u.outdent()

        u.blank_line(2)

        return u

    def _resolver(self):
        pass

    def render(self) -> IndentString:
        en = self._enum()
        return en


class PythonEnumRenderer(FileRenderer):
    def render_header(self):
        u = IndentString()
        u.add('import enum')
        u.add('from hatsudenki.packages.master.model import EnumResolver, MasterResolver')
        u.blank_line(2)
        return u

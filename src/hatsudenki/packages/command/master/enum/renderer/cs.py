from hatsudenki.packages.command.master.enum.data import EnumData
from hatsudenki.packages.command.renderer.base import RenderUnit, FileRenderer
from hatsudenki.packages.command.stdout.output import IndentString


class CSEnumRenderUnit(RenderUnit[EnumData]):
    def render(self):
        u = IndentString()

        if self.data.is_out_desc:
            u.add(f'using System.ComponentModel;')
            u.blank_line()

        u.add(f'public enum Enum{self.data.full_classname}')
        u.indent('{')
        for v in self.data.values:
            if self.data.is_out_desc:
                u.add(f'{v.cs_desc}')
            u.add(f'{v.cs_str}')

        u.add(f'MAX = {len(self.data.values)},')
        u.outdent('}')
        return u


class CSEnumRenderer(FileRenderer):
    pass

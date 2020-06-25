from hatsudenki.packages.command.master.loader import MasterTableLoader
from hatsudenki.packages.command.master.table import MasterTable
from hatsudenki.packages.command.renderer.base import RenderUnit, FileRenderer
from hatsudenki.packages.command.stdout.output import IndentString, quote


class MasterDataStoreRenderUnit(RenderUnit[MasterTable]):
    def _model(self):
        u = IndentString()
        m = IndentString()
        f = IndentString()

        if self.data.range_key is not None:
            u.indent(f'class {self.data.class_name}(MasterModelMulti):')
        else:
            u.indent(f'class {self.data.class_name}(MasterModelSolo):')

        m.indent('class Meta:')
        m.add(f"table_name = '{self.data.table_name}'")
        m.add(
            f"primary_index = PrimaryIndex({', '.join([f'{quote(l.python_name)}' for l in self.data.cursor_labels])})")

        f.indent('class Field:')

        i = IndentString()
        i.indent('def __init__(self, **kwargs):')
        i.add('super().__init__(**kwargs)')
        i.add('fields = self.__class__.Field')

        resolver = IndentString()
        for k, c in self.data.columns.items():
            f.add(c.python_define_name)
            i.add(c.python_init_name)

            if c.type_name == 'master':
                # master
                resolver.add(c.resolver_name)
                resolver.blank_line()
            elif c.type_name == 'chose':
                # select/choseの解決
                resolver.add(c.resolver_name(self.data.columns[c.selector]))
                resolver.blank_line()
        resolver.add(self.data.log_id_resolver())

        m.blank_line()
        u.add(m)
        f.blank_line()
        u.add(f)
        u.add(i)

        if resolver.line_num > 0:
            u.blank_line()
            u.add(resolver)

        u.blank_line()
        return u

    def render(self):
        u = self._model()
        return u


class MasterModelRenderer(FileRenderer):

    def __init__(self, master_loader: MasterTableLoader):
        super().__init__()
        self.master_loader = master_loader

    def render_header(self) -> IndentString:
        u = IndentString()
        u.add('from hatsudenki.packages.cache.base import column')
        u.add('from hatsudenki.packages.cache.base.index import PrimaryIndex')
        u.add('from hatsudenki.packages.master.model import MasterModelSolo, MasterModelMulti')
        u.blank_line(2)
        return u

    def render_units(self):
        u = IndentString()
        [u.add(MasterDataStoreRenderUnit(table).render()) for k, table in self.master_loader.iter()]
        return u

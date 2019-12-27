from pathlib import Path

from hatsudenki.commands.base import BaseCommand
from hatsudenki.packages.command.files import write_file
from hatsudenki.packages.command.hatsudenki.loader import HatsudenkiLoader
from hatsudenki.packages.command.hatsudenki.renderer.tables import TableFileRenderer
from hatsudenki.packages.command.master.enum.loader import EnumLoader
from hatsudenki.packages.command.master.enum.renderer.excel import EnumExcelBook
from hatsudenki.packages.command.master.enum.renderer.python import PythonEnumRenderUnit, PythonEnumRenderer
from hatsudenki.packages.command.master.loader import MasterTableLoader
from hatsudenki.packages.command.master.renderer.excel import MasterExcelBook
from hatsudenki.packages.command.master.renderer.model import MasterModelRenderer
from hatsudenki.packages.command.stdout.output import ToolOutput


class Command(BaseCommand):

    def __init__(self, stdout=None, stderr=None, no_color=False):
        super().__init__(stdout, stderr, no_color)

    def add_arguments(self, parser):
        parser.add_argument('--dsl_path', '-dsl', type=str, required=True)
        parser.add_argument('--out', '-o', type=str, required=True)
        parser.add_argument('--clean', '-c', action='store_true')

    def clean(self, excel_path: Path, python_path: Path):
        ToolOutput.anchor('出力フォルダをクリア')
        ToolOutput.anchor('cleaning excel directory...')
        for book_file in excel_path.glob('*.xlsx'):
            ToolOutput.out(str(book_file))
            book_file.unlink()
        ToolOutput.pop('OK')

        ToolOutput.anchor('cleaning output python directory...')
        p = python_path / 'def_enum.py'
        if p.exists():
            p.unlink()
        p = python_path / 'masters.py'
        if p.exists():
            p.unlink()
        ToolOutput.pop('OK')
        ToolOutput.print_with_pop('OK')

    def load_enum_yaml(self, in_enum_path: Path):
        ToolOutput.anchor('loading enum yaml files...')
        enum_loader = EnumLoader(in_enum_path)
        enum_loader.setup()
        ToolOutput.pop('OK')
        return enum_loader

    def load_master_yaml(self, in_master_path: Path, enum_loader: EnumLoader):
        ToolOutput.anchor('loading master yaml files...')
        master_loader = MasterTableLoader(in_master_path, enum_loader)
        master_loader.setup()
        ToolOutput.pop('OK')
        return master_loader

    def render_dynamo(self, in_yaml_path: Path, out_python_path: Path):
        dynamo_loader = HatsudenkiLoader(in_yaml_path)
        dynamo_loader.setup()
        tbl_render = TableFileRenderer(dynamo_loader.iter(), str(out_python_path))
        write_file(out_python_path / 'tables.py', tbl_render.render())

    def render_master_excel(self, out_excel_path: Path, master_loader: MasterTableLoader):
        ToolOutput.anchor('generate excel books...')
        for excel_name, sheet_list in master_loader.ref_excel_name.items():
            ToolOutput.anchor(f'{excel_name}')
            book = MasterExcelBook(out_excel_path / (excel_name + '.xlsx'), sheet_list)
            ToolOutput.pop()
            book.save_file()
        ToolOutput.pop('OK')

    def render_enum_excel(self, out_excel_path: Path, enum_loader: EnumLoader, master_loader: MasterTableLoader):
        ToolOutput.anchor('generate enum excel...')
        enum_book = EnumExcelBook(out_excel_path / 'pg_excel' / '定義タイプ.xlsx', enum_loader, master_loader)
        enum_book.save_file()
        ToolOutput.pop('OK')

    def render_master_python(self, out_python_path: Path, master_loader: MasterTableLoader):
        ToolOutput.anchor('generate master python code...')
        model_renderer = MasterModelRenderer(master_loader)
        write_file(out_python_path / 'masters.py', model_renderer.render())
        ToolOutput.pop()

    def render_enum_python(self, out_python_path: Path, enum_loader: EnumLoader):
        ToolOutput.anchor('generate python enum code...')
        out_enum_path = out_python_path / 'def_enum.py'
        pr = PythonEnumRenderer()
        for p, d in enum_loader.iter():
            pr.add_unit(PythonEnumRenderUnit(d))
        write_file(out_enum_path, pr.render())
        ToolOutput.pop('OK')

    async def handle(self, *args, **options):
        ToolOutput.anchor('Generate Excel...')
        in_path = Path(options['dsl_path'])
        out_path = Path(options['out'])

        in_master_path = in_path / 'master'
        in_enum_path = in_path / 'enum'
        in_dynamo_path = in_path / 'dynamo'

        out_excel_path = out_path / 'excel'
        out_python_path = out_path / 'python'

        if options['clean']:
            self.clean(out_excel_path, out_python_path)

        enum_loader = self.load_enum_yaml(in_enum_path)
        master_loader = self.load_master_yaml(in_master_path, enum_loader)

        self.render_master_excel(out_excel_path, master_loader)

        # CSモデルの書き出し
        # out_cs_path = options.get('out_cs_path', None)
        # if out_cs_path:
        #     cs_model_path = Path(out_cs_path)
        #     ToolOutput.anchor('generate cs model...')
        #     for name, table in master_loader.iter():
        #         if not table.is_out_pack:
        #             continue
        #         p = cs_model_path / (table.class_name + '.cs')
        #         r = MasterCSModelRenderer()
        #         r.add_unit(MasterCSModelRenderUnit(table))
        #         write_file(p, r.render())
        #     ToolOutput.pop('OK')

        self.render_enum_excel(out_excel_path, enum_loader, master_loader)

        # pythonモデル＆データストアの書き出し
        self.render_master_python(out_python_path, master_loader)

        # generate python master models.
        self.render_enum_python(out_python_path, enum_loader)

        self.render_dynamo(in_dynamo_path, out_python_path)

        # generate cs master models.
        # if out_cs_path:
        #     ToolOutput.anchor('generate cs master models...')
        #     out_cs_enum_path = Path(out_cs_path) / 'enums'
        #     for p, d in enum_loader.iter():
        #         cs = CSEnumRenderer()
        #         cs.add_unit(CSEnumRenderUnit(d))
        #         write_file(out_cs_enum_path / ('Enum' + d.full_classname + '.cs'), cs.render())
        #     ToolOutput.pop('OK')
        ToolOutput.pop('all done!')

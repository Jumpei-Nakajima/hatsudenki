from pathlib import Path

import yaml

from hatsudenki.commands.base import BaseCommand
from hatsudenki.packages.command.files import recreate_dir, write_file
from hatsudenki.packages.command.master.enum.loader import EnumLoader
from hatsudenki.packages.command.master.exporter.loader import MasterExcelLoader
from hatsudenki.packages.command.master.exporter.renderer.raw_yaml import RawYamlRenderer
from hatsudenki.packages.command.master.loader import MasterTableLoader
from hatsudenki.packages.command.master.tag.loader import MasterTagLoader, MasterTag
from hatsudenki.packages.command.stdout.output import ToolOutput


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('-c', '--clean', action='store_true')
        parser.add_argument('-i', '--in', required=True)
        parser.add_argument('-o', '--out', required=True)
        parser.add_argument('-dsl', '--dsl_path', required=True)

        parser.add_argument('-t', '--tag', default='ALL')
        parser.add_argument('-d', '--debug', action='store_true')

    def clean(self, out_yaml_path: Path):
        recreate_dir(out_yaml_path)

    async def handle(self, *args, **options):
        # 引数解析
        in_path = Path(options.get('in'))
        out_path = Path(options.get('out'))
        dsl_path = Path(options.get('dsl_path'))
        target_tag: str = options.get('tag')
        enable_debug = options.get('debug', False)
        ToolOutput.print(f'dump master yaml... tag={target_tag} debug={enable_debug}', False)
        ToolOutput.out(f'parameter {in_path}, {out_path}, tag={target_tag}')

        # 出力TAG
        tag_loader = MasterTagLoader(dsl_path / 'define' / 'master_tag.yml')
        tag_loader.load()

        # TAG情報を取得
        out_tag = tag_loader.get_tag(target_tag)

        if out_tag is None:
            raise Exception(f'出力タグ {target_tag} は定義されていません！')

        if out_tag.is_only:
            ToolOutput.out(f'onlyモードで出力します include={out_tag.include}')

        enum_path = dsl_path / 'enum'
        master_schema_path = dsl_path / 'master'
        master_excel_path = in_path

        out_raw_yaml_path = out_path / 'raw_yaml'

        # Enum準備
        ToolOutput.print_with_anchor('enum準備')
        enum_loader = EnumLoader(enum_path)
        enum_loader.setup()
        ToolOutput.print_with_pop('OK')

        # スキーマパース
        ToolOutput.print_with_anchor('masterスキーマ準備')
        master_loader = MasterTableLoader(master_schema_path, enum_loader)
        master_loader.setup()
        ToolOutput.print_with_pop('OK')

        # Excel読み込み
        ToolOutput.print_with_anchor('excel準備')
        excel_loader = MasterExcelLoader(master_excel_path, master_loader, tag_loader)
        excel_loader.set_out_level(out_tag)
        excel_loader.set_debug_flg(enable_debug)
        excel_loader.setup()
        ToolOutput.print_with_pop('OK')

        # cleanフラグが設定されている場合は掃除する
        if options.get('clean', False):
            recreate_dir(out_raw_yaml_path)

        # YAML書き出し
        self.generate_yaml(excel_loader, out_raw_yaml_path, out_tag)

    def generate_yaml(self, excel_loader: MasterExcelLoader, out_raw_yaml_path: Path, out_tag: MasterTag):
        for p, excel in excel_loader.iter():
            r = RawYamlRenderer(excel)
            datas = r.render(out_tag)
            for k, v in datas.items():
                data = yaml.dump(v, allow_unicode=True, default_flow_style=False)
                write_file(out_raw_yaml_path / (k + '.yml'), data)

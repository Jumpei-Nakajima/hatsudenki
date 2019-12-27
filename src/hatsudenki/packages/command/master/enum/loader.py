from pathlib import Path

from hatsudenki.packages.command.loader.base import LoaderBase
from hatsudenki.packages.command.master.enum.data import EnumData
from hatsudenki.packages.command.stdout.output import path_to_snake


class EnumLoader(LoaderBase[EnumData]):
    def __init__(self, base_path: Path):
        super().__init__(base_path)
        self.ext = '.yml'
        self.ref_table_name = {}

    def _load(self, path: Path):
        return EnumData(self.base_path, path)

    def setup(self, dir_name=''):
        super().setup(dir_name)

        def gen_key(p) -> str:
            return 'master_' + path_to_snake(str(Path(p).relative_to(self.base_path))).replace('.yml', '')

        self.ref_table_name = {gen_key(p): p for p, v in self.datas.items()}

    def get_by_name(self, enum_name: str):
        if enum_name.startswith('enum_'):
            enum_name = enum_name.replace('enum_', 'master_')
        if not enum_name.startswith('master_'):
            enum_name = 'master_' + enum_name
        try:
            self.ref_table_name[enum_name]
        except Exception as e:
            raise Exception(f'{enum_name} is not found.')
        return self.get_from_path(self.ref_table_name[enum_name])

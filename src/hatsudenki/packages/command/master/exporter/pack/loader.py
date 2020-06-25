from pathlib import Path

from hatsudenki.packages.command.loader.base import LoaderBase, T
from hatsudenki.packages.command.master.exporter.pack.yaml import MasterExportYaml
from hatsudenki.packages.command.master.loader import MasterTableLoader
from hatsudenki.packages.command.stdout.output import ConsoleColor


class MasterExportYamlLoader(LoaderBase[MasterExportYaml]):
    def __init__(self, base_path: Path, master_loader: MasterTableLoader):
        super().__init__(base_path)
        self.ext = '.yml'
        self.master_loader = master_loader

    def _load(self, path: Path) -> T:
        table_name = path.name.replace('.yml', '')
        table = self.master_loader.get_by_table_name(table_name)
        if table is None:
            print(ConsoleColor.RED + f'[MASTER_Load] {table_name} は 定義されていません！！！' + ConsoleColor.END)
            return None

    def get_by_table_name(self, table_name: str):
        return self.get_from_path(self.base_path / (table_name + '.yml'))

from os import PathLike
from pathlib import Path
from typing import Dict

from hatsudenki.packages.command.loader.base import LoaderBase
from hatsudenki.packages.command.master.importer.yaml import MasterDataYaml
from hatsudenki.packages.command.master.loader import MasterTableLoader
from hatsudenki.packages.command.stdout.output import ToolOutput


class MasterDataYamlLoader(LoaderBase[MasterDataYaml]):

    def __init__(self, base_path: PathLike, loader: MasterTableLoader):
        super().__init__(base_path)
        self.ext = '.yml'
        self.master_loader = loader
        self.ref_table_name: Dict[str, MasterDataYaml] = {}

    def _load(self, path: Path):
        tbl_name = path.name.replace('.yml', '')
        ToolOutput.anchor(f'"{path}:1" をロード')
        table = self.master_loader.get_by_table_name(tbl_name)
        if table is None:
            ToolOutput.pop(f'みつからない {tbl_name}')
            return None

        a = MasterDataYaml(self.base_path, path, table, self)
        ToolOutput.pop('OK')
        return a

    def setup(self, dir_name=''):
        super().setup(dir_name)

        self.ref_table_name = {y.table_name: y for k, y in self.iter() if y is not None}

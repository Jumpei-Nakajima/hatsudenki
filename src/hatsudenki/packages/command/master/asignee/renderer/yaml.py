from dataclasses import dataclass

import yaml

from hatsudenki.packages.command.master.table import MasterTable


@dataclass
class MasterDataYamlRenderer:
    master_table: MasterTable

    def render(self):
        return yaml.dump(self.master_table.data, allow_unicode=True, default_flow_style=False)

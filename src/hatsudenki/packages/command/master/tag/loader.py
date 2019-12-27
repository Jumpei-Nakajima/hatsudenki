from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, List

from hatsudenki.packages.command.files import yaml_load


@dataclass
class MasterTag:
    name: str
    data: dict
    level: int
    include: List[str]
    is_debug: bool

    @property
    def is_only(self):
        return self.data.get('only')


class MasterTagLoader:
    def __init__(self, yaml_path: Path):
        self.yaml_path = yaml_path
        self._data: Optional[Dict[str, MasterTag]] = None

    def load(self):
        if self._data is None:
            self._data = {}
            d = yaml_load(self.yaml_path)

            for idx, t in enumerate(d):
                if 'debug_only' in t and t['debug_only'] is True:
                    self._data[t['name']] = MasterTag(name=t['name'], data=t, level=idx,
                                                      include=t.get('include', []), is_debug=True)
                else:
                    self._data[t['name']] = MasterTag(name=t['name'], data=t, level=idx, include=t.get('include', []),
                                                      is_debug=False)
                    self._data[t['name'] + '_debug'] = MasterTag(name=t['name'] + '_DEBUG', data=t, level=idx,
                                                                 include=t.get('include', []), is_debug=True)

    def get_tag(self, tag_name: str):
        return self._data.get(tag_name, None)

from pathlib import Path

from hatsudenki.packages.command.hatsudenki.data import HatsudenkiData
from hatsudenki.packages.command.loader.base import LoaderBase


class HatsudenkiLoader(LoaderBase[HatsudenkiData]):
    """
    HatsudenkiDSLローダー
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ext = '.yml'

    def _load(self, path: Path):
        return HatsudenkiData(self.base_path, path, self)

    def setup(self, dir_name=''):
        super().setup(dir_name)

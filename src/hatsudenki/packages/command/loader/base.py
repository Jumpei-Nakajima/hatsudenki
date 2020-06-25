import unicodedata
from pathlib import Path
from typing import TypeVar, Generic, Dict, Generator, Tuple

from hatsudenki.packages.command.stdout.output import ToolOutput

T = TypeVar('T')


class LoaderBase(Generic[T]):
    def __init__(self, base_path: Path):
        self.base_path = base_path.absolute()
        self.datas: Dict[Path, T] = {}
        self.ext = ''

    def setup(self, dir_name=''):
        ToolOutput.out(f'[{self.__class__.__name__}] setup...')
        self.datas = {Path(unicodedata.normalize('NFC', str(path))): None for path in
                      (self.base_path / dir_name).glob('**/*' + self.ext)}

    def iter(self) -> Generator[Tuple[Path, T], None, None]:
        for k, v in self.datas.items():
            yield k, self.get_from_path(k)

    def get_item(self, file: str) -> T:
        p = self.to_full_path(file)
        return self.get_from_path(p)

    def get_from_path(self, path: Path):
        p = Path(unicodedata.normalize('NFC', str(path.absolute())))
        d = self.datas.get(p)
        if d is None:
            d = self._load(p)
            if d is not None:
                self.datas[p] = d
        return d

    def exists(self, path: str):
        p = self.to_full_path(path)
        return p in self.datas

    def _load(self, path: Path) -> T:
        raise NotImplementedError()

    def to_full_path(self, path: str):
        p = Path(unicodedata.normalize('NFC', str(self.base_path / path)))
        return p

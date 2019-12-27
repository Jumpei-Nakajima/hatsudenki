import os
from itertools import chain
from pathlib import Path


class BaseInfo(object):
    def __init__(self, base_path: Path, full_path: Path, *args, **kwargs):
        bp = os.path.normpath(str(base_path))
        fp = os.path.normpath(str(full_path))
        self.full_path = fp
        self.rel_path: str = fp.replace(bp, '')
        self.dirs = self.rel_path.replace('.yml', '').lstrip(os.sep).split(os.sep)

        self.base_class_name = ''.join(
            [s.capitalize() for s in self.rel_path.replace('_', os.sep).split(os.sep)[2:]]).replace('.yml', '')

    @property
    def top_level(self):
        return self.dirs[0]

    @property
    def filename(self) -> str:
        return self.dirs[-1]

    @property
    def filename_without_ext(self):
        r = self.dirs[-1].split('.')
        if len(r) > 1:
            return ''.join(r[:-1])
        else:
            return r[0]

    @property
    def dirname(self) -> str:
        return os.path.dirname(self.full_path)

    @property
    def normalize_name(self):
        if len(self.dirs) > 1 and self.dirs[-1] == self.dirs[-2]:
            return '_'.join(self.dirs[0:-1])
        return '_'.join(self.dirs)

    @property
    def normalize_dirs(self):
        if len(self.dirs) > 1 and self.dirs[-1] == self.dirs[-2]:
            return self.dirs[0:-1]
        return self.dirs

    @property
    def full_classname(self):
        s = chain.from_iterable([[c.capitalize() for c in a.split('_')] for a in self.normalize_dirs])
        return ''.join(s)

    def __str__(self):
        return self.rel_path

    def __repr__(self):
        return self.rel_path

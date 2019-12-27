import csv
import os
import shutil
import tarfile
from collections import OrderedDict
from os import PathLike
from pathlib import Path
from typing import Iterator, Union

import yaml

# initialize yaml representer
from hatsudenki.packages.command.stdout.output import ToolOutput


def represent_odict(dumper, instance):
    return dumper.represent_mapping('tag:yaml.org,2002:map', instance.items())


yaml.add_representer(OrderedDict, represent_odict)


# initialize yaml constructor
def construct_odict(loader, node):
    return OrderedDict(loader.construct_pairs(node))


yaml.add_constructor('tag:yaml.org,2002:map', construct_odict)


def yaml_load(file_path: Path) -> Union[dict, list]:
    """
    load yaml file
    :param file_path: target file path
    """

    with file_path.open(encoding='utf-8') as file:
        ret = yaml.safe_load(file)

    return ret


def yaml_write(file_path: Path, data: Union[dict, list]):
    write_file(file_path, yaml.dump(data, allow_unicode=True, default_flow_style=False))


def write_file(out_path: Path, data: str, mode: str = 'w'):
    ToolOutput.print(f'write to {out_path}', False)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open(mode=mode, encoding='utf-8') as file:
        file.write(data)


def open_write_stream(file_path: Path, mode: str = 'w'):
    file_path.parent.mkdir(parents=True, exist_ok=True)
    return file_path.open(mode=mode, encoding='utf-8')


def text_load(file_path: Path):
    with file_path.open(mode='r', encoding='utf8') as file:
        text = file.read()
    return text


def write_or_ignore(out_path: PathLike, data: any, mode='w'):
    if os.path.exists(out_path):
        return
    write_file(out_path, data, mode)


def write_csv(out_path: PathLike, data: Iterator[any]):
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open(mode='w', encoding='utf8') as csv_file:
        w = csv.writer(csv_file)
        for l in data:
            w.writerow(l)


def recreate_dir(path: Path):
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)


def tar_compress(src: Path, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(dest, mode='w:gz') as t:
        for f in src.glob('**/*'):
            print(f.relative_to(src))
            t.add(f, str(f.relative_to(src)))


def tar_extract(src: Path, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(src, mode='r:gz') as t:
        t.extractall(dest)

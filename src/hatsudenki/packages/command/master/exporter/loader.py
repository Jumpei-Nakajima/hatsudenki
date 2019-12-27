import unicodedata
from pathlib import Path
from typing import Dict, Optional

from hatsudenki.packages.command.loader.base import LoaderBase, T
from hatsudenki.packages.command.master.exporter.excel import MasterExcel
from hatsudenki.packages.command.master.loader import MasterTableLoader
from hatsudenki.packages.command.master.tag.loader import MasterTagLoader, MasterTag


class MasterExcelLoader(LoaderBase[MasterExcel]):
    """
    Masterデータ入力Excelローダー
    """

    def __init__(self, base_path: Path, master_loader: MasterTableLoader, master_tag_loader: MasterTagLoader):
        super().__init__(base_path)
        self.ext = '.xlsx'
        self.master_loader = master_loader
        self.ref_excel_name: Dict[str, MasterExcel] = {}
        self.out_tag: Optional[MasterTag] = None
        self.tag_loader = master_tag_loader
        self.is_debug = False

    def _load(self, path: Path) -> T:
        table = self.master_loader.get_by_excel_name(path.name.replace('.xlsx', ''))

        return MasterExcel(self.base_path, path, table, self)

    def set_out_level(self, out_tag: MasterTag):
        self.out_tag = out_tag

    def set_debug_flg(self, debug_flg: bool):
        self.is_debug = debug_flg

    def resolve_tag(self, tag_str: str):
        return self.tag_loader.get_tag(tag_str)

    def setup(self, dir_name=''):
        self.datas = {Path(unicodedata.normalize('NFC', str(path))): None for path in
                      (self.base_path / dir_name).glob('*' + self.ext) if not path.name.startswith('~$')}
        self.ref_excel_name = {p.name: ex for p, ex in self.iter()}

    def get_by_excel_name(self, name: str):
        """
        Excelファイル名を指定して取得

        :param name: 取得する対象のExcelファイル名
        :return:
        """
        return self.ref_excel_name[name]

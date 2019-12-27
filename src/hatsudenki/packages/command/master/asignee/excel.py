from pathlib import Path

from hatsudenki.packages.command.excel.excel_data import ExcelData


class MasterAssignerExcel(ExcelData):
    def __init__(self, file_path: Path):
        super().__init__(file_path)

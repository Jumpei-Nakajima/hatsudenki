from pathlib import Path
from typing import Optional

from hatsudenki.packages.command.files import yaml_load
from hatsudenki.packages.command.stdout.output import snake_to_camel
from hatsudenki.packages.command.util.base_info import BaseInfo


class EnumValue(object):
    def __init__(self, val: int, data: dict):
        self.data = data
        self.id = val

    @property
    def name(self) -> str:
        return self.data['name']

    @property
    def enum_value_name(self):
        return self.name.upper()

    @property
    def excel_name(self):
        return self.data.get('ref_name', self.name)

    @property
    def display_name(self):
        return self.data.get('display_name', self.excel_name)

    @property
    def comment(self) -> str:
        return self.data.get('comment', '')

    @property
    def excel_row(self):
        return [self.id, self.name, self.excel_name, self.display_name, self.comment]

    @property
    def python_str(self):
        return f'{self.enum_value_name}: int = {self.id}'

    @property
    def cs_str(self):
        return f'{self.enum_value_name} = {self.id},'

    @property
    def cs_desc(self):
        return f'[Description("{self.display_name}")]'

    @property
    def ts_str(self):
        return f'{self.enum_value_name} = {self.id},'


class EnumValueDefine(EnumValue):
    pass


class EnumValueSelect(EnumValue):
    @property
    def excel_row(self):
        return [self.id, self.name, self.excel_name, self.display_name, self.select_value, self.comment]

    @property
    def select_value(self) -> Optional[str]:
        return self.data.get('value', None)

    @property
    def resolve_name(self):
        if self.select_value.startswith('enum_'):
            return f'if value == cls.{self.enum_value_name}: return EnumResolver({snake_to_camel(self.select_value)})'
        elif self.select_value.startswith('master_'):
            return f'if value == cls.{self.enum_value_name}: return MasterResolver(masters.{snake_to_camel(self.select_value)})'


class EnumData(BaseInfo):
    def __init__(self, base_path: Path, full_path: Path, *args, **kwargs):
        super().__init__(base_path, full_path, *args, **kwargs)
        self.data = yaml_load(full_path)

        tp = self.data.get('type', 'order')

        if tp == 'define':
            self.values = [EnumValueDefine(v['value'], v) for idx, v in enumerate(self.data['values'])]
        elif tp == 'selector':
            # selector
            self.origin = self.data.get('origin', 0)
            self.values = [EnumValueSelect(idx + self.origin, v) for idx, v in enumerate(self.data['values'])]
        else:
            self.origin = self.data.get('origin', 0)
            self.values = [EnumValue(idx + self.origin, v) for idx, v in enumerate(self.data['values'])]

    @property
    def label(self):
        return f'{self.excel_name}:{self.excel_sheet_name}:{self.rel_path}'

    @property
    def excel_name(self):
        return '定義タイプ'

    @property
    def excel_sheet_name(self):
        return self.data['name']

    @property
    def is_selector(self):
        return self.data.get('type', 'int') == 'selector'

    @property
    def is_out_desc(self) -> bool:
        return self.data.get('out_desc', False)

    def find(self, val, is_raw=False):
        # フォーマットの都合上必ず文字列で格納されているので、raw=trueの場合はintにキャストしてやる必要がある
        vv = int(val) if is_raw else val

        if is_raw:
            return next((v for v in self.values if v.id == vv), None)
        else:
            return next((v for v in self.values if v.excel_name == vv), None)

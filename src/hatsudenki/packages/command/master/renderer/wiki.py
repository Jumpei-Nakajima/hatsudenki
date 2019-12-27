import dataclasses
from collections import defaultdict
from dataclasses import dataclass
from typing import List, Dict, DefaultDict

from hatsudenki.packages.command.master.column import MasterColumn, ColumnEnum, ColumnRelation
from hatsudenki.packages.command.master.enum.loader import EnumLoader
from hatsudenki.packages.command.master.loader import MasterTableLoader
from hatsudenki.packages.command.master.table import MasterTable
from hatsudenki.packages.command.network.request import aio_http_get_json, aio_http_post_json, aio_http_put_json
from hatsudenki.packages.command.renderer.base import RenderUnit, FileRenderer
from hatsudenki.packages.command.stdout.output import IndentString, ToolOutput


@dataclass()
class GitWikiUploader:
    base_url: str
    token_str: str

    def _make_header(self):
        return {
            'PRIVATE-TOKEN': self.token_str
        }

    async def get_page_list(self):
        resp = await aio_http_get_json(self.base_url, self._make_header())
        return resp

    async def create_page(self, content: str, page_name: str):
        """
                gitlabのwikiの記事作成

                :param content: ページ内容
                :param page_name: 作成ページ名
                :return: レスポンス
                """
        body = {
            'content': content,
            'title': page_name,
            'format': 'markdown'
        }
        resp = await aio_http_post_json(f'{self.base_url}', body, self._make_header())
        return resp

    async def update_page(self, content: str, page_name: str):
        """
        gitlabのwikiの記事更新

        :param content: ページ内容
        :param page_name: 更新ページ名
        :return: レスポンス
        """
        body = {
            'title': page_name,
            'format': 'markdown',
            'content': content
        }
        slug = page_name.replace('/', '%2F')
        resp = await aio_http_put_json(f'{self.base_url}/{slug}', body, self._make_header())
        return resp


@dataclass()
class MarkdownTableRenderer:
    header: List[str]
    rows: List[Dict[str, str]] = dataclasses.field(default_factory=list)

    def append(self, row: Dict[str, str]):
        self.rows.append(row)

    def render(self):
        u = IndentString()
        u.add('|' + '|'.join(self.header) + '|')
        u.add('|' + '|'.join(['---------'] * len(self.header)) + '|')

        for row in self.rows:
            s = []
            for h in self.header:
                s.append(self._get(row.get(h, '----')))
            u.add('|' + '|'.join(s) + '|')
        return u

    def _get(self, d):
        return d if d else '----'


class MasterDataWikiRenderUnit(RenderUnit[MasterTable]):
    def render(self):
        ToolOutput.anchor(f'{self.data.label} をMarkdownに変換')
        u = IndentString()
        u.add(f'# {self.data.excel_sheet_name}')
        u.add(f'## 情報')
        u.add(self.data.description if self.data.description else '*!! descriptionが設定されていません !!*')
        u.blank_line()

        u.add(self._gen_info().render())

        u.add(f'## 担当者')
        u.add(self._gen_assigner().render())

        u.add(f'## スキーマ情報')
        u.add(self._column().render())

        ToolOutput.pop('OK')
        return u

    @property
    def page_name(self):
        return f'MasterData/{self.data.excel_name}'

    def get_page_name(self, data: MasterTable):
        return f'MasterData/{data.excel_name}'

    def _label(self, data: MasterTable):
        return f'【{data.excel_name}】{self.data.excel_sheet_name}'

    def _gen_assigner(self):
        tr = MarkdownTableRenderer(['セクション', '担当者'])
        tr.append({'セクション': 'クライアント', '担当者': ', '.join(self.data.assignee_clients)})
        tr.append({'セクション': 'サーバー', '担当者': ', '.join(self.data.assignee_servers)})
        tr.append({'セクション': 'プランナー', '担当者': ', '.join(self.data.assignee_planners)})
        return tr

    def _gen_info(self):
        tr = MarkdownTableRenderer(['情報', '値'])
        tr.append({'情報': 'テーブル名', '値': self.data.table_name})
        tr.append({'情報': 'スキーマ定義ファイル', '値': self.data.rel_path})
        tr.append({'情報': 'PACK出力', '値': self._bool_to_str(self.data.is_out_pack, '○', '☓')})
        return tr

    def _bool_to_str(self, val: bool, true_str: str, false_str: str):
        return true_str if val else false_str

    def _column(self):
        tr = MarkdownTableRenderer(['名称', 'カラム名', 'index', '型', '入力制限', 'コメント', 'PACK出力'])

        for k, c in self.data.columns.items():
            tr.append({
                '名称': c.excel_raw_header_name,
                'カラム名': c.column_name,
                'index': 'hash' if c.is_hash_key else 'range' if c.is_range_key else None,
                '型': self._type_to_str(c),
                '入力制限': self._limit_to_str(c),
                'コメント': c.comment,
                'PACK出力': self._bool_to_str(c.is_no_pack, '☓', '○')
            })
        return tr

    def _type_to_str(self, column: MasterColumn):
        if type(column) is ColumnEnum:
            return f'enum<br>{column.to}'
        if type(column) is ColumnRelation:
            return f'master<br>{column.to}'

        return column.type_name

    def _limit_to_str(self, column: MasterColumn):
        ret = []
        l = column.value_limit
        if l is None:
            return None
        for t, v in l.items():
            if t == 'choice':
                for vv in v:
                    ret.append(f'- {vv.excel_name}')
            elif t == 'master':
                ret.append(f'[[{self.get_page_name(v)}#{v.excel_sheet_name}]]')
        return '<br />'.join(ret)

    def render_index(self):
        return {
            'Book': f'[{self.data.excel_name}#{self.data.excel_sheet_name}](/{self.page_name}#{self.data.excel_sheet_name})',
            'Table': self.data.table_name,
            'Yaml': self.data.rel_path,
            'description': self.data.description,
            'Client': '<br />'.join(self.data.assignee_clients),
            'Server': '<br />'.join(self.data.assignee_servers),
            'Planner': '<br />'.join(self.data.assignee_planners)
        }


@dataclass
class MasterDataWikiRenderer(FileRenderer):
    master_loader: MasterTableLoader
    enum_loader: EnumLoader

    def render_header(self) -> IndentString:
        return super().render_header()

    def render_units(self):
        ToolOutput.anchor('Wiki出力を開始')
        u = IndentString()
        for k, table in self.master_loader.iter():
            un = MasterDataWikiRenderUnit(table).render()
            u.add(un)
        ToolOutput.pop('OK')

        return u

    def render_index(self, index: MarkdownTableRenderer):
        ToolOutput.anchor('目次')
        u = IndentString()
        u.add('# MasterData目次')
        u.add(index.render())
        return u

    async def render_to_wiki(self, base_url, token_str):
        ToolOutput.anchor('Wiki出力を開始')

        wiki = GitWikiUploader(base_url, token_str)

        page_list = await wiki.get_page_list()
        page_slug_list = [v['slug'] for v in page_list]

        d: DefaultDict[str, List[MasterDataWikiRenderUnit]] = defaultdict(list)
        ToolOutput.anchor('YAML読み込み')
        for k, table in self.master_loader.iter():
            ru = MasterDataWikiRenderUnit(table)
            ToolOutput.out(f'{ru.page_name} # {ru.data.excel_sheet_name}')
            d[ru.page_name].append(ru)
        ToolOutput.pop('OK')

        ToolOutput.anchor('書き出し')
        num = 1
        index = MarkdownTableRenderer(['ID', 'Book', 'Client', 'Server', 'Planner', 'Table'])
        for p, rus in d.items():
            ToolOutput.anchor(p)
            u = IndentString()
            for r in rus:
                u.add(r.render())
                idx = r.render_index()
                idx['ID'] = str(num)
                num += 1
                index.append(idx)

            if p in page_slug_list:
                ToolOutput.print(f'{p}更新します')
                await wiki.update_page(content=u.render(), page_name=p)
            else:
                ToolOutput.print(f'{p}作成します')
                await wiki.create_page(content=u.render(), page_name=p)
            ToolOutput.pop('OK')

        ix = self.render_index(index)

        index_title = 'MasterData/☆INDEX☆'
        if index_title in page_slug_list:
            ToolOutput.print(f'{index_title}更新します')
            await wiki.update_page(content=ix.render(), page_name=index_title)
        else:
            ToolOutput.print(f'{index_title}作成します')
            await wiki.create_page(content=ix.render(), page_name=index_title)

        ToolOutput.pop('OK')

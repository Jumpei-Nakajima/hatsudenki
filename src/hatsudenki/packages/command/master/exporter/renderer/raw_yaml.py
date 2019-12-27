from hatsudenki.packages.command.master.exporter.excel import MasterExcel
from hatsudenki.packages.command.master.tag.loader import MasterTag


class RawYamlRenderer(object):

    def __init__(self, excel_book: MasterExcel):
        self.book = excel_book

    def render(self, out_tag: MasterTag):
        ret = {}
        for sheet in self.book.iter_table_sheet():
            li = []

            for r in sheet.iter():
                # この列内にNoneが含まれてたらエラーにしてやる
                for key, value in r.items():
                    if value is None:
                        raise Exception(
                            f"エクセル:{sheet.book.filename} のシート:{sheet.sheet.title} の{key}に空データが存在します"
                        )

                li.append(r)

            ret[sheet.table.table_name] = li

        return ret

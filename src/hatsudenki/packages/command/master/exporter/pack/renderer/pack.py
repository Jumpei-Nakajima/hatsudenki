import struct

import msgpack
from lz4 import block

from hatsudenki.packages.command.master.exporter.pack.yaml import MasterExportYaml


class PackRenderer(object):
    def __init__(self, yaml_data: MasterExportYaml):
        self.data = yaml_data

    def render(self):
        cpy = []
        key_order = [c.column_name for c in self.data.table.columns.values() if not c.is_no_pack]
        key_order.extend([c.column_name for c in self.data.table.shadow_columns.values() if not c.is_no_pack])

        for row_idx, row in enumerate(self.data.data):
            c = []
            for key in key_order:
                c.append(row[key])
            cpy.append(c)

        pack = msgpack.packb([cpy])
        pack_len = len(pack)
        # ブロックフォーマットで圧縮。MsgPackにサイズを付与するため、compressバイナリには含めない
        comp = block.compress(pack, mode='default', store_size=False)

        # ヘッダ作成
        # Pythonでは明示的にExt32が指定できない？ので自前で突っ込む
        t = struct.pack('B', 0xc9)
        # サイズ情報がタイプ＋4バイト付与されるため、5バイト膨らむ
        t += struct.pack('>I', len(comp) + 5)
        # msgpack-csharpは99番をLZ4コンテナとして使っているようだ
        t += struct.pack('B', 99)
        # 解凍後サイズを頭に突っ込む
        t += struct.pack('B', 0xd2)
        t += struct.pack('>I', pack_len)
        # ヘッダとボディを結合
        t += comp

        return t

    def yaml_render(self):
        ret = []
        for row_idx, row in enumerate(self.data.data):
            ret.append(row)
        return ret

import asyncio
import itertools
import pprint
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from pprint import pprint
from typing import List, Dict, Type, TypeVar

from hatsudenki.packages.client import HatsudenkiClient
from hatsudenki.packages.table.define import TableType
from hatsudenki.packages.table.solo import SoloHatsudenkiTable


@dataclass
class BatchGetTask:
    table_name: str = None
    keys: dict = None


class BatchWriteKind(Enum):
    Put = 'PutRequest'
    Delete = 'DeleteRequest'


@dataclass
class BatchWriteTask:
    table_name: str = None
    item: dict = None
    kind: BatchWriteKind = None

    @property
    def query(self):
        return {
            self.kind.value: {'Item': self.item}
        } if self.kind is BatchWriteKind.Put else {
            self.kind.value: {'Key': self.item}
        }

    @property
    def real_table_name(self):
        return HatsudenkiClient.resolve_table_name(self.table_name)


class QueryBatchGetItem(object):
    def __init__(self):
        self._task: List[BatchGetTask] = []

    def append(self, table: SoloHatsudenkiTable):
        self._task.append(
            BatchGetTask(table_name=table._collection_name, keys=table.serialized_key))

    def exec_query(self, limit=100):
        query = defaultdict(lambda: {'Keys': []})
        w = 0
        for task in self._task:
            query[HatsudenkiClient.resolve_table_name(task.table_name)]['Keys'].append(task.keys)
            w += 1
            if w >= limit:
                r = dict(query)
                w = 0
                query.clear()
                yield HatsudenkiClient.batch_get_item(r)
        if w is not 0:
            yield HatsudenkiClient.batch_get_item(dict(query))

    async def exec(self, limit=100):
        # TODO: unprocessへの対応がまだ
        ret = defaultdict(list)

        [[ret[key].extend(v) for key, v in r.items()] for r in
         await asyncio.gather(*[q for q in self.exec_query(limit)])]

        return BatchGetResponse(ret)

    @property
    def task_num(self):
        return len(self._task)


class QueryBatchWriteItem(object):
    """
    Batch処理（書き込み）
    """

    def __init__(self):
        self._task: List[BatchWriteTask] = []

    def append(self, task: BatchWriteTask):
        """
        タスク追加

        :param task: 追加するタスクインスタンス
        :return:
        """
        self._task.append(task)

    def append_create(self, table: SoloHatsudenkiTable):
        """
        CREATEのタスクを追加

        :param table: 追加するタスクインスタンス
        :return:
        """
        self._task.append(
            BatchWriteTask(table_name=table._collection_name, item=table.serialize(), kind=BatchWriteKind.Put))

    def append_delete(self, table: SoloHatsudenkiTable):
        """
        DELETEのタスクを追加

        :param table: 追加するタスクインスタンス
        :return:
        """
        t = BatchWriteTask(table_name=table.get_collection_name(), item=table.serialized_key,
                           kind=BatchWriteKind.Delete)
        self._task.append(t)

    def exec_query(self, limit=25):
        task_len = len(self._task)
        chunk_heads = (r * limit for r in range(task_len // limit + (0 < (task_len % limit))))

        def group(head):
            return itertools.groupby(self._task[head:(head + limit)], lambda x: x.real_table_name)

        def query(head):
            return {k: [val.query for val in v] for k, v in group(head)}

        return (HatsudenkiClient.batch_write_item(query(h)) for h in chunk_heads)

    async def exec(self, limit=25):
        g = [q for q in self.exec_query(limit)]
        await asyncio.gather(*g)

    @property
    def task_num(self):
        return len(self._task)


T = TypeVar('T')


class BatchGetResponse(object):
    def __init__(self, result: Dict[str, List[any]]):
        self.result = result

    def __repr__(self):
        return pprint.pformat(self.result)

    def get(self, target_table_type: Type[T]) -> List[T]:
        col_items = self.result[HatsudenkiClient.resolve_table_name(target_table_type.get_collection_name())]
        tt = target_table_type.get_table_type()

        if tt in {TableType.RootTable, TableType.SingleSoloTable, TableType.SingleMultiTable}:
            yield from (target_table_type.deserialize(c) for c in col_items)
            return

        for item in col_items:
            if tt is TableType.ChildTable:
                if item[target_table_type.get_range_key_name()]['S'].startswith(target_table_type.get_tag_name()):
                    yield target_table_type.deserialize(item)
            elif tt is TableType.ChildSoloTable:
                if item[target_table_type.get_range_key_name()]['S'] == target_table_type.get_tag_name():
                    yield target_table_type.deserialize(item)

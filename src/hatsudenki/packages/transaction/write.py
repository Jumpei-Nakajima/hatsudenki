from dataclasses import dataclass
from enum import Enum
from typing import List

from hatsudenki.packages.client import HatsudenkiClient
from hatsudenki.packages.expression.update import UpdateExpression
from hatsudenki.packages.table.solo import SoloHatsudenkiTable


class TransactionWriteKind(Enum):
    Put = 'Put'
    Delete = 'Delete'
    Update = 'Update'


@dataclass
class TransactWriteTask:
    table_name: str = None
    item: dict = None
    condition: str = None


MAX_TRANSACTION_ITEM = 10


class QueryTransactWriteItem(object):
    def __init__(self):
        self._task: List[dict] = []

    def _check(self):
        if len(self._task) >= MAX_TRANSACTION_ITEM:
            raise Exception(f'transaction item too many. max {MAX_TRANSACTION_ITEM}')

    def append_put(self, table: SoloHatsudenkiTable, overwrite=False):
        self._check()
        cond = table.not_exist_condition() if not overwrite else None

        query = {
            TransactionWriteKind.Put.value: {
                'TableName': HatsudenkiClient.resolve_table_name(table.get_collection_name()),
                'Item': table.serialize(),

                **(cond.to_parameter() if cond is not None else {})
            }
        }

        self._task.append(query)

    def append_delete(self, table: SoloHatsudenkiTable):
        self._check()
        query = {
            TransactionWriteKind.Delete.value: {
                'TableName': HatsudenkiClient.resolve_table_name(table.get_collection_name()),
                'Key': table.serialized_key,
            }
        }

        self._task.append(query)

    def append_update(self, table: SoloHatsudenkiTable):
        self._check()
        upd = UpdateExpression()

        table.build_update_expression(upd)
        cond = table.exist_condition()

        upd.add('_v', 1)
        cond.op_and()
        with cond:
            cond.equal('_v', table._v)
            cond.op_or()
            cond.attribute_exists('_v')
        key = table.serialized_key

        query = {
            TransactionWriteKind.Update.value: {
                'TableName': HatsudenkiClient.resolve_table_name(table.get_collection_name()),
                'Key': key,
                **(upd.to_parameter(cond))
            }
        }

        self._task.append(query)

    async def exec(self):
        res = await HatsudenkiClient.transaction_write(self._task)
        return res

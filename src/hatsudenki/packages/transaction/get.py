from enum import Enum
from typing import List, Type, TypeVar

from hatsudenki.packages.client import HatsudenkiClient
from hatsudenki.packages.table.solo import SoloHatsudenkiTable

MAX_TRANSACTION_ITEM = 10


class TransactionGetKind(Enum):
    Get = 'Get'


class QueryTransactGetItem(object):
    def __init__(self):
        self._task: List[dict] = []
        pass

    def _check(self):
        if len(self._task) >= MAX_TRANSACTION_ITEM:
            raise Exception(f'transaction item too many. max {MAX_TRANSACTION_ITEM}')

    def append(self, table: SoloHatsudenkiTable):
        self._check()
        query = {
            TransactionGetKind.Get.value: {
                'TableName': HatsudenkiClient.resolve_table_name(table.get_collection_name()),
                'Key': table.serialized_key
            }
        }

        self._task.append(query)

    async def exec(self):
        res = await HatsudenkiClient.transaction_get(self._task)
        return TransactGetResult(res)


T = TypeVar('T')


class TransactGetResult(object):
    def __init__(self, res: List[dict]):
        self._result = res

    def get(self, idx: int, target_type: Type[T]) -> T:
        return target_type.deserialize(self._result[idx]['Item'])

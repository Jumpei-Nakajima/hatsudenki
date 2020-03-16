import asyncio
from uuid import uuid4

from hatsudenki.packages.batch import QueryBatchWriteItem, QueryBatchGetItem
from hatsudenki.packages.client import HatsudenkiClient
from out.tables import table_setup, ExampleChild


async def main():
    print('hello!')
    
    p = await HatsudenkiClient.list_tables()
    print(f'all_tables={len(p)}')
    
    print('generate uuid.')
    uuids = [uuid4() for _ in range(10)]

    print('batch write')
    q = QueryBatchWriteItem()
    for u in uuids:
        q.append_create(ExampleChild(user_id=u, child_range_value=10))
    await q.exec()
    print('batch write ok.')
    
    print('batch get')
    q2 = QueryBatchGetItem()
    for u in uuids:
        q2.append(ExampleChild(user_id=u, child_range_value=10))
    a = await q2.exec()
    n = a.get(ExampleChild)
    for r in n:
        print(r.to_dict())
        


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    HatsudenkiClient.setup(loop, 'http://localhost:8000', '', 'ap-northeast-1', 'hoge', 'fuga', 10)
    table_setup()
    loop.run_until_complete(main())
    loop.run_until_complete(HatsudenkiClient.die())

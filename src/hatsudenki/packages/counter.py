from collections import defaultdict, Counter
from dataclasses import dataclass, field
from pprint import pprint
from typing import Dict, DefaultDict, Counter as Counter_type

QUERY_COUNTER_LABEL_DEFINE = [
    'get',
    'put',
    'update',
    'query',
    'batch_write',
    'batch_get',
    'transact_write',
    'transact_get'
]


@dataclass
class QueryCounterUnit:
    counters: Dict[str, DefaultDict[str, int]] = field(default_factory=dict)
    total_counter: Counter_type = field(default_factory=Counter)
    read_ccu_counter: Counter_type = field(default_factory=Counter)
    read_ccu_detail_dict: Dict = field(default_factory=dict)
    total_read_ccu: float = 0
    write_ccu_counter: Counter_type = field(default_factory=Counter)
    write_ccu_detail_dict: Dict = field(default_factory=dict)
    total_write_ccu: float = 0

    def count(self, label, key='unknown'):
        counter = self.counters.get(label, None)
        if counter is None:
            self.counters[label] = defaultdict(int)
            counter = self.counters[label]
        counter[key] += 1
        self.total_counter[label] += 1

    def count_read_ccu(self, info: Dict):
        self.read_ccu_counter[info['TableName']] += info['CapacityUnits']
        self.total_read_ccu += info['CapacityUnits']
        b_target = self.read_ccu_detail_dict.get(info['TableName'], False)
        if b_target is False:
            self.read_ccu_detail_dict[info['TableName']] = defaultdict(int)
        self.read_ccu_detail_dict[info['TableName']]['Table'] += info['Table']['CapacityUnits']
        gsi_info = info.get('GlobalSecondaryIndexes', False)
        if gsi_info is False:
            return
        for key, dic in info['GlobalSecondaryIndexes'].items():
            self.read_ccu_detail_dict[info['TableName']][key] += dic['CapacityUnits']

    def count_write_ccu(self, info: Dict):
        self.write_ccu_counter[info['TableName']] += info['CapacityUnits']
        self.total_write_ccu += info['CapacityUnits']
        b_target = self.write_ccu_detail_dict.get(info['TableName'], False)
        if b_target is False:
            self.write_ccu_detail_dict[info['TableName']] = defaultdict(int)
        self.write_ccu_detail_dict[info['TableName']]['Table'] += info['Table']['CapacityUnits']
        gsi_info = info.get('GlobalSecondaryIndexes', False)
        if gsi_info is False:
            return
        for key, dic in info['GlobalSecondaryIndexes'].items():
            self.write_ccu_detail_dict[info['TableName']][key] += dic['CapacityUnits']

    def init_counter(self):
        self.counters = dict()
        self.total_counter = Counter()
        self.read_ccu_counter = Counter()
        self.read_ccu_detail_dict = dict()
        self.total_read_ccu = 0
        self.write_ccu_counter = Counter()
        self.write_ccu_detail_dict = dict()
        self.total_write_ccu = 0


class QueryCounter:
    _counter: QueryCounterUnit = None

    @classmethod
    def _get_context_data(cls):
        if cls._counter is None:
            cls._counter = QueryCounterUnit()
        return cls._counter

    @classmethod
    def count(cls, label, key='unknown'):
        query_counter = cls._get_context_data()
        query_counter.count(label, key)

    @classmethod
    def count_read_ccu(cls, info: Dict):
        query_counter = cls._get_context_data()
        query_counter.count_read_ccu(info)

    @classmethod
    def count_write_ccu(cls, info: Dict):
        query_counter = cls._get_context_data()
        query_counter.count_write_ccu(info)

    @classmethod
    def init_counter(cls):
        query_counter = cls._get_context_data()
        query_counter.init_counter()

    @classmethod
    def dump_counter(cls):
        query_counter = cls._get_context_data()
        pprint(query_counter.counters)
        pprint(query_counter.total_counter)
        print('-read consume cu-')
        pprint(query_counter.read_ccu_counter)
        print(f'total:{query_counter.total_read_ccu}')
        print('-write consume cu-')
        pprint(query_counter.write_ccu_counter)
        print(f'total:{query_counter.total_write_ccu}')
        print('-read detail-')
        pprint(query_counter.read_ccu_detail_dict)
        print('-write detail-')
        pprint(query_counter.write_ccu_detail_dict)

    @classmethod
    def get_consumed_cu(cls):
        query_counter = cls._get_context_data()
        return query_counter.total_read_ccu, query_counter.total_write_ccu

    @classmethod
    def get_cu_detail_counters(cls):
        query_counter = cls._get_context_data()
        return query_counter.read_ccu_detail_dict, query_counter.write_ccu_detail_dict

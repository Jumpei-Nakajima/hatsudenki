from hatsudenki.packages.cache.base.memory.dynamic.multi import DynamicCacheBaseTableMulti
from hatsudenki.packages.cache.base.memory.dynamic.solo import DynamicCacheBaseTableSolo
from hatsudenki.packages.cache.base.memory.dynamic.sync.multi import SyncDynamicCacheBaseTableMulti
from hatsudenki.packages.cache.base.memory.dynamic.sync.solo import SyncDynamicCacheBaseTableSolo


class InMemoryTableSolo(DynamicCacheBaseTableSolo):
    def __init_subclass__(cls, **kwargs):
        from hatsudenki.packages.cache.inmemory.manager import InMemoryTableManager
        InMemoryTableManager.register(cls)


class InMemoryTableMulti(DynamicCacheBaseTableMulti, DynamicCacheBaseTableSolo):
    pass


class SyncInMemoryTableSolo(SyncDynamicCacheBaseTableSolo):
    def __init_subclass__(cls, **kwargs):
        from hatsudenki.packages.cache.inmemory.manager import InMemoryTableManager
        InMemoryTableManager.register(cls)


class SyncInMemoryTableMulti(SyncDynamicCacheBaseTableMulti, DynamicCacheBaseTableSolo):
    pass

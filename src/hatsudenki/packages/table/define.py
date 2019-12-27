from enum import Enum, auto


class TableType(Enum):
    RootTable = auto()
    SingleSoloTable = auto()
    SingleMultiTable = auto()
    ChildTable = auto()
    ChildSoloTable = auto()

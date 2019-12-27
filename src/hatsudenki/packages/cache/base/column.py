from datetime import datetime
from typing import Generic, TypeVar

T = TypeVar('T')


class BaseMasterColumn(Generic[T]):
    def __init__(self, name: str):
        self.name = name

    def convert(self, value: any) -> T:
        return value


class MasterColumnInt(BaseMasterColumn[int]):

    def convert(self, value: any) -> T:
        return int(value)


class MasterColumnString(BaseMasterColumn[str]):
    def convert(self, value: any) -> T:
        return str(value)


class MasterColumnEnum(BaseMasterColumn[int]):
    def convert(self, value: any) -> T:
        return int(value)


class MasterColumnRelation(BaseMasterColumn[str]):

    def __init__(self, name: str, to: str):
        super().__init__(name)
        self.to = to


class MasterColumnDate(BaseMasterColumn[datetime]):

    def convert(self, value: any) -> T:
        return datetime.fromtimestamp(value)


class MasterColumnSelect(BaseMasterColumn[int]):
    pass


class MasterColumnChose(BaseMasterColumn[str]):
    pass

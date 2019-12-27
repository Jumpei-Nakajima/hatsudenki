from __future__ import annotations

from typing import Optional, List

from attr import dataclass


@dataclass
class Schema:
    @dataclass
    class Group:
        @dataclass
        class Table:
            @dataclass
            class Column:
                name: str

                def print(self):
                    print(f"        {self.name}")

                def print_str(self):
                    return f"        - {self.name}"

            name: str
            columns: List[Column]

            def __sub__(self, other: Schema.Group.Table) -> Optional[Schema.Group.Table]:
                if other is None:
                    return self
                if self.name != other.name:
                    raise Exception('Can not sub')
                diff_column = [c for c in self.columns if c.name not in [o.name for o in other.columns]]
                if len(diff_column) == 0:
                    return None
                return Schema.Group.Table(self.name, diff_column)

            def print(self):
                print(f"    {self.name}")
                [c.print() for c in self.columns]

            def print_str(self):
                res = f"    - {self.name} \r \n" + " \r \n".join([c.print_str() for c in self.columns])
                return res


        name: str
        tables: List[Table]

        def __sub__(self, other: Schema.Group) -> Optional[Schema.Group]:
            if other is None:
                return self
            if self.name != other.name:
                raise Exception('Can not sub')
            diff_gen = (s - next((t for t in other.tables if t.name == s.name), None) for s in self.tables)
            diff_tbls = [t for t in diff_gen if t is not None]
            if len(diff_tbls) == 0:
                return None
            return Schema.Group(self.name, diff_tbls)

        def print(self):
            print(f"{self.name}")
            [t.print() for t in self.tables]

        def print_str(self):
            return f" - {self.name} \r \n" + " \r \n".join([t.print_str() for t in self.tables])

    groups: List[Group]

    def __sub__(self, other: Schema) -> Optional[Schema]:
        diff_gen = (g - next((o for o in other.groups if o.name == g.name), None) for g in self.groups)
        diff = [g for g in diff_gen if g is not None]
        if len(diff) == 0:
            return None
        return Schema(diff)

    def print(self):
        [g.print() for g in self.groups]

    def print_str(self):
        return " \r \n".join([g.print_str() for g in self.groups])

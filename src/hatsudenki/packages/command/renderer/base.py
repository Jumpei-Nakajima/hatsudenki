from __future__ import annotations

from typing import Generic, TypeVar

from hatsudenki.packages.command.stdout.output import IndentString

T = TypeVar('T')


class FileRenderer(object):
    def __init__(self):
        self.units = []

    def render_header(self) -> IndentString:
        return IndentString()

    def add_unit(self, *unit: RenderUnit):
        [self.units.append(u) for u in unit]

    def render_units(self):
        u = IndentString()
        [u.add(un.render()) for un in self.units]
        return u

    def render(self):
        body = IndentString()
        body.add(self.render_header())
        body.add(self.render_units())

        return body.render()


class RenderUnit(Generic[T]):
    def __init__(self, data: T):
        self.data = data

    def render(self) -> IndentString:
        return IndentString()

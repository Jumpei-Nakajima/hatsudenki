import os
import re
import sys
import traceback
from itertools import chain
from typing import List, Union, Tuple


def snake_to_camel(value: str):
    if value is None:
        return None
    # まず全て小文字にする
    ret = value.lower()

    words = ret.split('_')
    words = [w.capitalize() for w in words]
    return ''.join(words)


def parse_type_str(type_str: str):
    brace = type_str.find('(')
    brace_str = None
    if brace != -1:
        s = type_str[:brace]
        brace_str = type_str[brace + 1:-1]
    else:
        s = type_str
    return (s, brace_str)


def path_to_snake(path: str, separator=os.sep):
    path = erase_ext(path)
    a = list(path.split(separator))
    if len(a) >= 2 and a[-1] == a[-2]:
        a.pop()
    res = chain.from_iterable([s.split('_') for s in a])
    r = '_'.join(res)
    return r


def erase_ext(file_name: str):
    return ''.join(file_name.split('.')[0:-1])


class ConsoleColor:
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    PURPLE = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    END = '\033[0m'
    BOLD = '\038[1m'
    UNDERLINE = '\033[4m'
    INVISIBLE = '\033[08m'
    REVERCE = '\033[07m'


def color_print(color: str, text: str, indent=0):
    prefix = ''
    if indent >= 1:
        prefix = '  ' * (indent - 1) + (' └')
    text = prefix + text
    try:
        print(color + text + ConsoleColor.END)
    except UnicodeEncodeError:
        sys.stdout.buffer.write((text + '\n').encode('utf-8'))


class ToolOutput(object):
    TOOL_COLOR_LEVEL = [
        ConsoleColor.BLUE,
        ConsoleColor.GREEN,
        ConsoleColor.PURPLE,
        ConsoleColor.YELLOW,
        ConsoleColor.CYAN,
    ]

    out_indent = 0
    stack = []
    is_setup = False

    @classmethod
    def setup(cls):
        if not cls.is_setup:
            cls.is_setup = True
            cls.out_indent = 0
            cls.stack = []

    @classmethod
    def indent(cls, text=None):
        if cls.is_setup is False:
            return
        if text:
            cls.print(text)
        cls.out_indent += 1

    @classmethod
    def outdent(cls, text=None):
        if cls.is_setup is False:
            return
        cls.out_indent -= 1
        if text:
            cls.print(text)

    @classmethod
    def clear(cls):
        if cls.is_setup is False:
            return
        cls.out_indent = 0

    @classmethod
    def anchor(cls, text=None):
        if cls.is_setup is False:
            return
        cls.stack.append(cls.out_indent)
        if text is not None:
            cls.print(text)

    @classmethod
    def out(cls, text):
        if cls.is_setup is False:
            return
        cls.print(text, False)

    @classmethod
    def pop(cls, text=None):
        if cls.is_setup is False:
            return
        if text is not None:
            cls.print(text)
        cls.out_indent = cls.stack.pop()

    @classmethod
    def get_color(cls):
        return cls.TOOL_COLOR_LEVEL[cls.out_indent % len(cls.TOOL_COLOR_LEVEL)]

    @classmethod
    def print(cls, text: str, with_indent=True):
        if cls.is_setup is False:
            return
        for idx, t in enumerate(text.split('\n')):
            if idx is 0:
                color_print(cls.get_color(), t, cls.out_indent)
            else:
                color_print(cls.get_color(), '  ' * cls.out_indent + t)

        if with_indent:
            cls.indent()

    @classmethod
    def print_error(cls, text: str, with_indent=True):
        sys.stdout.flush()
        if cls.is_setup is False:
            return

        t2 = '【☆ERROR☆】 ' + text
        for idx, t in enumerate(t2.split('\n')):
            if idx is 0:
                color_print(ConsoleColor.RED, t, cls.out_indent)
            else:
                color_print(ConsoleColor.RED, '  ' * cls.out_indent + t)

        if with_indent:
            cls.indent()

    @classmethod
    def out_error(cls, text: str):
        if cls.is_setup is False:
            return
        cls.print_error(text, False)

    @classmethod
    def print_exc(cls, exc: Exception):
        sys.stdout.flush()
        if cls.is_setup is False:
            return
        color_print(ConsoleColor.RED, str(exc))
        color_print(ConsoleColor.RED, traceback.format_exc())

    @classmethod
    def print_with_anchor(cls, text: str):
        cls.anchor()
        cls.print(text)

    @classmethod
    def print_with_pop(cls, text: str):
        cls.print(text)
        cls.pop()

    @classmethod
    def print_list(cls, text_list: List[str], with_indent=True):
        [cls.print(t, False) for t in text_list]
        if with_indent:
            cls.indent()

    @classmethod
    def block(cls, label: str):
        class _():
            def __enter__(self):
                cls.anchor()
                cls.print(label)

            def __exit__(self, exc_type, exc_val, exc_tb):
                cls.pop()

        return _()


def quote(s: str, quote_str='\''):
    return quote_str + s + quote_str


def indent_str_list(str_list: List[str], indent=1, indent_str='    '):
    return [(indent_str * indent) + s for s in str_list]


def capitalize_join(strs: List[str], sep: str = ''):
    return sep.join([st.capitalize() for st in strs])


def parse_bracket(type_str: str):
    brace = type_str.find('(')
    brace_str = None
    if brace != -1:
        s = type_str[:brace]
        brace_str = type_str[brace + 1:-1]
    else:
        s = type_str
    return s, brace_str


IndentChild = Union[str, 'IndentString']
IndentUnit = Tuple[int, IndentChild]


class IndentString(object):

    def __init__(self, header: IndentChild = None, indent_str='    '):
        self._children: List[IndentUnit] = []
        self._indent_str = indent_str
        self._indent_level = 0

        if header is not None:
            self.add(header)
            self.indent()

    @property
    def line_num(self):
        return len(self._children)

    @property
    def indent_str(self):
        return self._indent_level * self._indent_str

    def add(self, *child: Union['IndentString', str]):
        [self._children.append((self._indent_level, c)) for c in child if c is not None]

    def blank_line(self, num=1):
        [self._children.append((0, '')) for _ in range(num)]

    def indent(self, header: Union['IndentString', str] = None):
        if header is not None:
            self.add(header)
        self._indent_level += 1

    def outdent(self, footer: Union['IndentString', str] = None):
        self._indent_level -= 1
        if footer is not None:
            self.add(footer)

    def render(self, offset_indent=0, sep='\n'):
        def _henkan(c: IndentUnit):
            lv, u = c
            lv += offset_indent

            tp = type(u)
            if tp is str:
                return (self._indent_str * lv) + u
            elif isinstance(u, IndentString):
                return u.render(offset_indent=lv)

        return sep.join(map(_henkan, self._children))


class TAGText(IndentString):

    def __init__(self, header: IndentChild = None, indent_str='    '):
        super().__init__(header, indent_str)
        self._anchor = []

    def tag(self, tag: str):
        self.indent(f'<{tag}>')
        self._anchor.append((self._indent_level, tag))

    def end_tag(self):
        lv, tag = self._anchor.pop()
        self.outdent(f'</{tag}>')

    def tag_with_content(self, tag: str, content: str):
        self.add(f'<{tag}>{content}</{tag}>')


_underscore1 = re.compile(r'(.)([A-Z][a-z]+)')
_underscore2 = re.compile('([a-z0-9])([A-Z])')


def camel_to_snake(s: str):
    subbed = _underscore1.sub(r'\1_\2', s)
    return _underscore2.sub(r'\1_\2', subbed).lower()

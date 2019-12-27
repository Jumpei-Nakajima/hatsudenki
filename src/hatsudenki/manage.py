import argparse
import asyncio
import importlib
import sys
from pathlib import Path
from traceback import print_exc

from hatsudenki.commands.base import BaseCommand
from hatsudenki.packages.command.stdout.output import ToolOutput


def get_args():
    command_src = (p.name.replace('.py', '') for p in Path(__file__).with_name('commands').glob('*.py') if
                   not p.name.startswith('__'))
    parser = argparse.ArgumentParser(description='execute managed commands.')
    sub_parsers = parser.add_subparsers()

    for com in command_src:
        if com == 'base':
            continue
        mod = importlib.import_module('hatsudenki.commands.' + com)
        if not hasattr(mod, 'Command'):
            print(f'Command is not defined in {com}.py')
            continue
        sub_parser = sub_parsers.add_parser(com, description=getattr(mod.Command, 'help', 'no description.'))
        com_ins: BaseCommand = mod.Command()
        sub_parser.add_argument('-q', '--quiet', action='store_true', help='no output log')
        com_ins.add_arguments(sub_parser)
        sub_parser.set_defaults(handler=com_ins.handle)

    return parser


def cli():
    # 引数の処理
    parser = get_args()
    args = parser.parse_args()
    if hasattr(args, 'handler'):
        s = vars(args)

        # quietがセットされていない場合はToolのロガーをセットアップする
        if not s.get('quiet'):
            ToolOutput.setup()

        acv = asyncio.get_event_loop()
        try:
            # commandの実行
            acv.run_until_complete(args.handler(*sys.argv, **s))
        except:
            print_exc()
    else:
        # helpの表示（一応）
        parser.print_help()


if __name__ == '__main__':
    cli()

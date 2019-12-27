class BaseCommand(object):
    def __init__(self, stdout=None, stderr=None, no_color=False):
        pass

    async def handle(self, *args, **options):
        print('hoge')

    def add_arguments(self, parser):
        pass

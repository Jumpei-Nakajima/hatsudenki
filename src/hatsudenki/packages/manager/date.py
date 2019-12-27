from datetime import datetime


class DateResolver(object):
    def get_now(self):
        raise NotImplementedError()


class DefaultDateResolver(DateResolver):
    def get_now(self):
        return datetime.now()


class DateManager:
    _resolver = DateResolver()

    @classmethod
    def set_resolver(cls, resolver: DateResolver):
        cls._resolver = resolver

    @classmethod
    def get_now(cls):
        return cls._resolver.get_now()

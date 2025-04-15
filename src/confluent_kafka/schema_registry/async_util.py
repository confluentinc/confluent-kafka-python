"""
Utility functions for async operations.
"""

def asyncinit(cls):
    """
    Decorator to make a class async-initializable.
    """
    __new__ = cls.__new__

    async def init(obj, *arg, **kwarg):
        await obj.__init__(*arg, **kwarg)
        return obj

    def new(klass, *arg, **kwarg):
        obj = __new__(klass)
        coro = init(obj, *arg, **kwarg)
        return coro

    cls.__new__ = new
    return cls

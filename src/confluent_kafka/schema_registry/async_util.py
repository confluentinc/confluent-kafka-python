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

    def new(cls, *arg, **kwarg):
        obj = __new__(cls, *arg, **kwarg)
        coro = init(obj, *arg, **kwarg)
        #coro.__init__ = lambda *_1, **_2: None
        return coro

    cls.__new__ = new
    return cls

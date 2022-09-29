from typing import TypeVar

K = TypeVar("K")
T = TypeVar("T")


class ActionLock:
    """
    ActionLock is a class that can be used to lock a resource for a specific operation.
    For example, when emitting an event, we might need to ignore the same event in the process
    because its limit ran out. However, if we do that directly, we will get a error because
    we are modifying the dictionary while iterating over it. This class allows us to lock
    the dictionary for the duration of the iteration, and then unlock it when we are done.
    """

    def __init__(self):
        self.levels = 0
        self.callbacks: list[tuple[callable, tuple, dict]] = []

    def __enter__(self):
        self.levels += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.levels -= 1
        if self.levels == 0:
            for callback, args, kwargs in self.callbacks:
                callback(*args, **kwargs)
            self.callbacks = []

    def run(self, callback, *args, **kwargs):
        if self.levels == 1:
            callback(*args, **kwargs)
        else:
            self.callbacks.append((callback, args, kwargs))

    def setitem(self, dct: dict[K, T], key: K, value: T):
        def callback():
            dct[key] = value

        self.run(callback)

    def delitem(self, dct: dict[K, any], key: K):
        def callback():
            if key in dct:
                del dct[key]

        self.run(callback)

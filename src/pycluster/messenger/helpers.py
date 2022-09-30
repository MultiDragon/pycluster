from pycluster.messenger.message_object import MessageObject


def listen(event: int | str, *args, limit: int = -1, priority: float = 0, **kwargs):
    """
    Decorator for event listeners. The decorated method will be called when the event is emitted on the cluster.
    Caveats due to implementation details (same for math):
    1) it is not possible to ignore() events made by this decorator during __init__(), use post_init
    2) has to be re-decorated again if it's overridden by an inheriting class.
    :param event: the event name to listen for
    :param args: additional arguments to pass to the method
    :param limit: the number of times to listen for the event. -1 for unlimited.
    :param priority: the priority of the listener. Higher priority listeners are called first.
    :param kwargs: additional keyword arguments to pass to the method
    :return: the decorator for the method
    """

    class Listener:
        def __init__(self, callback):
            self.callback = callback

        def __set_name__(self, owner, name):
            old_init = owner.__init__

            def new_init(obj: MessageObject, *iargs, **ikwargs):
                old_init(obj, *iargs, **ikwargs)
                obj.listen_to(event, self.callback, limit=limit, pass_object=True, priority=priority, *args, **kwargs)

            owner.__init__ = new_init

            setattr(owner, name, self.callback)

    return Listener


def math(target: int | str, *args, limit: int = -1, priority: float = 0, **kwargs):
    """
    Decorator for math handlers. The decorated method will be called when the math recalculation is requested.
    NOTE: see caveats for listen()
    :param target: the recalculation target name to listen for
    :param args: additional arguments to pass to the method
    :param limit: the number of times to listen for the event. -1 for unlimited.
    :param priority: the priority of the listener. Higher priority calculators are called later.
    :param kwargs: additional keyword arguments to pass to the method
    :return: the decorator for the method
    """

    class Calculator:
        def __init__(self, callback):
            self.callback = callback

        def __set_name__(self, owner, name):
            old_init = owner.__init__

            def new_init(obj: MessageObject, *iargs, **ikwargs):
                old_init(obj, *iargs, **ikwargs)
                obj.register_math(
                    target, self.callback, limit=limit, pass_object=True, priority=priority, *args, **kwargs
                )

            owner.__init__ = new_init

            setattr(owner, name, self.callback)

    return Calculator


def replace(funcname: int | str, *args, limit: int = -1, priority: float = 0, **kwargs):
    """
    Decorator for method replacers.
    NOTE: see caveats for listen()
    :param funcname: the callback name to replace
    :param args: additional arguments to pass to the method
    :param limit: the number of times to listen for the event. -1 for unlimited.
    :param priority: the priority of the listener. Higher priority listeners are called first.
    :param kwargs: additional keyword arguments to pass to the method
    :return: the decorator for the method
    """

    class Replacer:
        def __init__(self, callback):
            self.callback = callback

        def __set_name__(self, owner, name):
            old_init = owner.__init__

            def new_init(obj: MessageObject, *iargs, **ikwargs):
                old_init(obj, *iargs, **ikwargs)
                obj.register_replace(
                    funcname, self.callback, limit=limit, pass_object=True, priority=priority, *args, **kwargs
                )

            owner.__init__ = new_init

            setattr(owner, name, self.callback)

    return Replacer


def post_init(*args, **kwargs):
    """
    Decorator for post_init methods. The decorated method will be called after the object is fully initialized.
    :param args: additional arguments to pass to the method
    :param kwargs: additional keyword arguments to pass to the method
    :return: the decorator for the method
    """

    class PostInit:
        def __init__(self, callback):
            self.callback = callback

        def __set_name__(self, owner, name):
            old_init = owner.__init__

            def new_init(obj: MessageObject, *iargs, **ikwargs):
                old_init(obj, *iargs, **ikwargs)
                self.callback(obj, *args, **kwargs)

            owner.__init__ = new_init

            setattr(owner, name, self.callback)

    return PostInit

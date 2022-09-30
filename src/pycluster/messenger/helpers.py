from pycluster.messenger.message_object import MessageObject


def listen(event: int | str, *args, limit=-1, **kwargs):
    """
    Decorator for event listeners. The decorated method will be called when the event is emitted on the cluster.
    Caveats due to implementation details (same for math):
    1) it is not possible to ignore() events made by this decorator during __init__(), use post_init
    2) has to be re-decorated again if it's overridden by an inheriting class.
    :param event: the event name to listen for
    :param args: additional arguments to pass to the method
    :param limit: the number of times to listen for the event. -1 for unlimited.
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
                obj.listen_to(event, self.callback, limit=limit, pass_object=True, *args, **kwargs)

            owner.__init__ = new_init

            setattr(owner, name, self.callback)

    return Listener


def math(event: int | str, *args, limit=-1, **kwargs):
    """
    Decorator for math handlers. The decorated method will be called when the math recalculation is requested.
    NOTE: see caveats for listen()
    :param event: the recalculation target name to listen for
    :param args: additional arguments to pass to the method
    :param limit: the number of times to listen for the event. -1 for unlimited.
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
                obj.register_math(event, self.callback, limit=limit, pass_object=True, *args, **kwargs)

            owner.__init__ = new_init

            setattr(owner, name, self.callback)

    return Calculator


def replace(funcname: int | str, *args, limit=-1, **kwargs):
    """
    Decorator for method replacers.
    NOTE: see caveats for listen()
    :param funcname: the callback name to replace
    :param args: additional arguments to pass to the method
    :param limit: the number of times to listen for the event. -1 for unlimited.
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
                registry = obj.registry
                registry.register_replace(funcname, obj, self.callback, pass_object=True, limit=limit, *args, **kwargs)

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

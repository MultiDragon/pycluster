import logging
from typing import Type, TypeVar

from pycluster.messenger.message_object import MessageObject, WrappedObject

T = TypeVar("T", bound=MessageObject)


class ObjectRegistry:
    logger = logging.getLogger("pycluster.messenger.ObjectRegistry")

    def __init__(self, name: str, ctype: Type[MessageObject] = None, forgiving: bool = False):
        self.name = name
        self.forgiving = forgiving
        self.objects: dict[int, Type[MessageObject]] = {}
        self.replaced_methods: dict[str, dict[MessageObject, callable]] = {}
        if ctype:
            self.bind(0, ctype)

    def bind(self, object_type: int, cls: Type[MessageObject]):
        self.objects[object_type] = cls
        cls.object_type = object_type

    def register(self, object_type: int):
        def decorator(cls: Type[MessageObject]):
            self.bind(object_type, cls)
            return cls

        return decorator

    def create_object(self, object_type: int, parent: MessageObject, **kwargs) -> MessageObject:
        object_cls = self.objects.get(object_type)
        if object_cls is None:
            if self.forgiving:
                if object_type >= 0:  # object_type = -1 indicates that this type was already lost during unwrapping
                    self.logger.warning(f"Unknown object type {object_type} in registry {self.name}")
                return MessageObject(parent, **kwargs)
            else:
                raise ValueError(f"Object type {object_type} is not registered")
        return object_cls(parent, **kwargs)

    def create_and_insert(
        self, object_type: int, parent: MessageObject, object_id: int | str, cast_to: Type[T] = MessageObject, **kwargs
    ) -> T:
        obj = self.create_object(object_type, parent, **kwargs)
        parent.add_child(object_id, obj)
        return obj

    def unwrap(self, wrapped: WrappedObject, parent: MessageObject = None) -> MessageObject:
        object_type, datagram, children_wrapped = wrapped
        obj = self.create_object(object_type, parent)
        obj._registry = self
        obj.unwrap(wrapped)
        return obj

    def replaceable(self, name: int | str):
        def decorator(method: callable):
            def decorated(*args, **kwargs):
                if name in self.replaced_methods:
                    replacements = self.replaced_methods[name]
                    if replacements:
                        obj, *cb = next(iter(replacements.values()))
                        value = obj.run_replace(name, cb, *args, **kwargs)
                        return value

                return method(*args, **kwargs)

            return decorated

        return decorator

    def register_replace(
        self,
        name: int | str,
        obj: MessageObject,
        method: callable,
        limit: int = -1,
        pass_object: bool = False,
        *args,
        **kwargs,
    ):
        if name not in self.replaced_methods:
            self.replaced_methods[name] = {}
        self.replaced_methods[name][obj] = (obj, method, limit, args, kwargs, pass_object)

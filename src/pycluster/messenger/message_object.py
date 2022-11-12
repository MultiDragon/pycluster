import logging
import queue
from typing import Optional, Sequence, TypeVar, TYPE_CHECKING

from pycluster.util.action_lock import ActionLock

if TYPE_CHECKING:
    from pycluster.messenger.object_registry import ObjectRegistry

WrappedChildren = dict[str, "WrappedObject"]
WrappedObject = tuple[int, any, WrappedChildren]
CallbackDefinition = tuple[callable, int, Sequence, dict, bool, float]
ObjectCallbackDefinition = tuple["MessageObject", callable, int, Sequence, dict, bool, float]
CallbackDict = dict["MessageObject", CallbackDefinition]

V = TypeVar("V")


FizzleReplace = object()
"""
    A special value that can be returned from a replacement method to indicate that
    the method did not have its condition fulfilled. The system will choose another suitable replacement.
"""


class MessageObject:
    logger = logging.getLogger("pycluster.messenger.MessageObject")

    object_type: int = -1
    children: dict[str, "MessageObject"]
    parent: "MessageObject"
    _ls_storage = None
    _mt_storage = None
    _rm_storage = None
    _act_lock = None
    _registry = None

    def __init__(self, parent: "MessageObject" = None, **kwargs):
        self.children = {}
        self.parent = parent

    def __getitem__(self, item) -> "MessageObject":
        return self.children[str(item)]

    def get(self, item) -> Optional["MessageObject"]:
        return self.children.get(str(item))

    def __iter__(self):
        return iter(self.children.items())

    def __contains__(self, item):
        return str(item) in self.children

    # Managing parent interaction
    @property
    def parent_cluster(self) -> "MessageObject":
        """
        Gets the parent cluster of this object. Will iterate multiple layers if needed.
        :return: the MessageCluster object.
        """

        if self.parent is not None:
            return self.parent.parent_cluster

        return self

    @property
    def action_lock(self) -> ActionLock:
        """
        Gets the listener lock for the parent cluster.
        :return: The listener lock.
        """

        parent = self.parent_cluster
        if parent is self:
            if self._act_lock is None:
                self._act_lock = ActionLock()
            return self._act_lock
        return parent.action_lock

    # Managing data
    @property
    def datagram(self):
        """
        Get the data for this object. Can be any type, will be properly saved from the object.
        :return: The data for this object.
        """

        return None

    @datagram.setter
    def datagram(self, value):
        """
        Set the data for this object. Can be any type, will be properly loaded into the object.
        :param value: The data for this object.
        """

    @property
    def registry(self) -> "ObjectRegistry":
        """
        Gets the registry of this object.
        :return: The registry.
        """

        if self._registry:
            return self._registry
        return self.parent_cluster.registry

    # Managing hierarchy
    def add_child(self, child_id: str, child: "MessageObject", allow_subtrees: bool = False) -> "MessageObject":
        """
        Adds a child to this object. The child is assumed to have the same parent cluster as this object.
        :param child_id: The id of the child.
        :param child: The child object.
        :param allow_subtrees: Whether to allow the child to have children from a different cluster.
        """

        if not allow_subtrees:
            assert child.parent_cluster is self.parent_cluster
        self.children[child_id] = child
        return child

    def remove_child(self, child_id: str) -> None:
        """
        Remove a child from this object and cleanup any listeners hanging on it.
        :param child_id: The id of the child.
        :return: nothing
        """

        child = self.children.pop(child_id, None)
        if child:
            child.cleanup()

    def wrap(self) -> WrappedObject:
        """
        Wraps the object, allowing it to be recreated elsewhere.
        :return: The wrapped object representation.
        """

        children_wrapped = {child_id: child.wrap() for child_id, child in self.children.items()}
        return self.object_type, self.datagram, children_wrapped

    def unwrap(self, wrapped: WrappedObject) -> None:
        """
        Unwrap a wrapped object and create any children objects that are needed.
        :param wrapped: The wrapped object to unwrap.
        :return: nothing
        """
        q: queue.Queue[tuple["MessageObject", str, int, any, WrappedChildren]] = queue.Queue()
        self.datagram = wrapped[1]
        for child_id, (child_type, data, children) in wrapped[2].items():
            q.put((self, child_id, child_type, data, children))

        while not q.empty():
            parent, child_id, child_type, data, children = q.get()
            if child_id in parent.children:
                child = parent.children[child_id]
            else:
                child = parent.registry.create_object(child_type, parent)
                parent.add_child(child_id, child)
            child.datagram = data
            for child_id, (child_type, data, children) in children.items():
                q.put((child, child_id, child_type, data, children))

    def copy_inplace(self, new_id: str = None) -> "MessageObject":
        """
        Copy this object and all children and attach it to the same cluster.
        Does not copy **kwargs, unless those are saved in a datagram.
        :return: The copied object.
        """
        wrapped = self.wrap()
        new = self.__class__(self.parent)
        new.unwrap(wrapped)
        if new_id is not None:
            self.parent.add_child(new_id, new)
        return new

    def copy(self) -> "MessageObject":
        """
        Copy this object and all children. Attach them to a different cluster.
        Does not copy **kwargs, unless those are saved in a datagram.
        :return: The copied object.
        """
        wrapped = self.wrap()
        new = self.__class__()
        new.unwrap(wrapped)
        return new

    # Top-level registration
    def __get_storage(self, name) -> dict[str, CallbackDict]:
        parent = self.parent_cluster
        if parent is self:
            if getattr(self, name) is None:
                setattr(self, name, {})
            return getattr(self, name)
        return parent.__get_storage(name)

    @staticmethod
    def __run_method(
        storage: Optional[CallbackDict], obj: "MessageObject", quad: CallbackDefinition, *args, **kwargs
    ) -> tuple[any, int]:
        callback, limit, cargs, ckwargs, pass_obj, priority = quad
        if pass_obj:
            value = callback(obj, *cargs, *args, **ckwargs, **kwargs)
        else:
            value = callback(*cargs, *args, **ckwargs, **kwargs)
        if storage:
            storage[obj] = callback, limit - 1, cargs, ckwargs, pass_obj, priority
        return value, limit - 1

    def __setup_listener(
        self,
        storage: dict[str, CallbackDict],
        event: int | str,
        callback: callable,
        *args,
        limit: int = 0,
        pass_object: bool = False,
        priority: int = 0,
        **kwargs
    ) -> None:
        with self.action_lock as lock:
            if event not in storage:
                storage[event] = {}
            lock.setitem(storage[event], self, (callback, limit, args, kwargs, pass_object, priority))

    def __ignore_listener(self, storage: dict[str, CallbackDict], event: int | str) -> None:
        with self.action_lock as lock:
            if event not in storage:
                return

            lock.delitem(storage[event], self)

    # Bulk Cleanup methods
    def ignore_all(self):
        """
        Ignore all events and math targets.
        :return: nothing
        """

        with self.action_lock as lock:
            for storage in [self.listener_storage, self.math_storage, self.repl_storage]:
                for event in storage:
                    lock.delitem(storage[event], self)

    def cleanup(self):
        """
        Cleanup the object.
        :return: nothing
        """

        self.ignore_all()
        for child in self.children.values():
            child.cleanup()
        self.children = {}

    # Event storages
    @property
    def listener_storage(self) -> dict[str, CallbackDict]:
        """
        Gets the storage used for listener events.
        """

        return self.__get_storage("_ls_storage")

    @property
    def math_storage(self) -> dict[str, CallbackDict]:
        """
        Gets the storage used for mathematical recalculations.
        """

        return self.__get_storage("_mt_storage")

    @property
    def repl_storage(self) -> dict[str, CallbackDict]:
        """
        Gets the storage used for method replacements.
        """

        return self.__get_storage("_rm_storage")

    # Event listeners
    def listen_to(self, *args, **kwargs) -> None:
        """
        Listen to an event on this object.
        """
        self.__setup_listener(self.listener_storage, *args, **kwargs)

    def register_math(self, *args, **kwargs) -> None:
        """
        Register a mathematical recalculation on this object.
        """
        self.__setup_listener(self.math_storage, *args, **kwargs)

    def register_replace(self, *args, **kwargs) -> None:
        """
        Register a method replacement on this object.
        """
        self.__setup_listener(self.repl_storage, *args, **kwargs)

    # Event ignores
    def ignore(self, *args, **kwargs) -> None:
        """
        Ignore an event on this object.
        """
        self.__ignore_listener(self.listener_storage, *args, **kwargs)

    def ignore_math(self, *args, **kwargs) -> None:
        """
        Ignore a mathematical recalculation on this object.
        """
        self.__ignore_listener(self.math_storage, *args, **kwargs)

    def ignore_replacement(self, *args, **kwargs) -> None:
        """
        Ignore a method replacement on this object.
        """
        self.__ignore_listener(self.repl_storage, *args, **kwargs)

    # Event emitters
    def emit(self, event: int | str, *args, **kwargs) -> None:
        """
        Emit an event to the parent cluster.
        :param event: The event to emit.
        :param args: The args to pass to the callback.
        :param kwargs: The kwargs to pass to the callback.
        :return: nothing
        """

        with self.action_lock:
            storage = self.listener_storage
            if event not in storage:
                return

            for obj, quad in sorted(storage[event].items(), key=lambda x: x[1][5], reverse=True):
                new_limit = self.__run_method(storage[event], obj, quad, *args, **kwargs)[1]
                if new_limit == 0:
                    obj.ignore(event)

    def calculate(self, target: int | str, init_value: V, **kwargs) -> V:
        """
        Emit an event to the parent cluster.
        :param target: The event to emit.
        :param init_value: The initial value without any calculation changes.
        :param kwargs: The kwargs to create the calculation context.
        :return: the resultant value
        """

        current_value = init_value
        with self.action_lock:
            storage = self.math_storage
            if target not in storage:
                return current_value

            for obj, quad in sorted(storage[target].items(), key=lambda x: x[1][5]):
                current_value, new_limit = self.__run_method(
                    storage[target], obj, quad, current_value, init_value=init_value, **kwargs
                )
                if new_limit == 0:
                    obj.ignore_math(target)

        return current_value

    def run_replace(self, name: int | str, *args, **kwargs):
        methods = self.repl_storage.get(name, {})
        with self.action_lock:
            for obj, cb in sorted(methods.items(), key=lambda x: x[1][5], reverse=True):
                value, limit = self.__run_method(None, obj, cb, *args, **kwargs)
                if value is FizzleReplace:
                    continue

                methods[obj] = cb[0], limit, cb[2], cb[3], cb[4], cb[5]
                if limit == 0:
                    self.ignore_replacement(name)
                return True, value

        return False, None

import logging
import queue
from typing import Optional, Sequence, TypeVar

from pycluster.util.action_lock import ActionLock

WrappedChildren = dict[int | str, "WrappedObject"]
WrappedObject = tuple[int, any, WrappedChildren]
CallbackDefinition = tuple[callable, int, Sequence, dict, bool]
CallbackDict = dict["MessageObject", CallbackDefinition]

V = TypeVar("V")


class MessageObject:
    logger = logging.getLogger("pycluster.messenger.MessageObject")

    object_type: int = -1
    children: dict[int | str, "MessageObject"]
    parent: "MessageObject"
    _ls_storage = None
    _mt_storage = None
    _act_lock = None
    _registry = None

    def __init__(self, parent: "MessageObject" = None, **kwargs):
        self.children = {}
        self.parent = parent

    def __getitem__(self, item):
        return self.children[item]

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
    def registry(self):
        """
        Gets the registry of this object.
        :return: The registry.
        """

        if self._registry:
            return self._registry
        return self.parent_cluster.registry

    # Managing hierarchy
    def add_child(self, child_id: int | str, child: "MessageObject"):
        """
        Adds a child to this object. The child is assumed to have the same parent cluster as this object.
        :param child_id: The id of the child.
        :param child: The child object.
        """

        assert child.parent_cluster is self.parent_cluster
        self.children[child_id] = child

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
        q: queue.Queue[tuple["MessageObject", int | str, int, any, WrappedChildren]] = queue.Queue()
        self.datagram = wrapped[1]
        for child_id, (child_type, data, children) in wrapped[2].items():
            q.put((self, child_id, child_type, data, children))

        while not q.empty():
            parent, child_id, child_type, data, children = q.get()
            child = self.parent_cluster.registry.create_object(child_type, parent)
            parent.add_child(child_id, child)
            child.datagram = data
            for child_id, (child_type, data, children) in children.items():
                q.put((child, child_id, child_type, data, children))

    def copy_inplace(self, new_id: int | str = None):
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

    def copy(self):
        """
        Copy this object and all children. Attach them to a different cluster.
        Does not copy **kwargs, unless those are saved in a datagram.
        :return: The copied object.
        """
        wrapped = self.wrap()
        new = self.__class__()
        new.unwrap(wrapped)
        return new

    # Managing events
    @property
    def listener_storage(self) -> dict[str, CallbackDict]:
        """
        Gets the listener storage for the parent cluster.
        :return: The listener storage.
        """

        parent = self.parent_cluster
        if parent is self:
            if self._ls_storage is None:
                self._ls_storage = {}
            return self._ls_storage
        return parent.listener_storage

    def listen_to(
        self, event: int | str, callback: callable, *args, limit: int = -1, pass_object: bool = False, **kwargs
    ):
        """
        Listen to an event from the parent cluster.
        :param event: The event to listen to.
        :param callback: The callback to call when the event is emitted.
        :param args: The args to pass to the callback.
        :param limit: The number of times to call the callback. -1 for infinite.
        :param pass_object: Whether to pass the object owner to the callback. Has to be True when using the decorator.
        :param kwargs: The kwargs to pass to the callback.
        :return: nothing
        """

        with self.action_lock as lock:
            storage = self.listener_storage
            if event not in storage:
                storage[event] = {}
            lock.setitem(storage[event], self, (callback, limit, args, kwargs, pass_object))

    @staticmethod
    def run_method(
        storage: Optional[CallbackDict], obj: "MessageObject", quad: CallbackDefinition, *args, **kwargs
    ) -> tuple[any, int]:
        callback, limit, cargs, ckwargs, pass_obj = quad
        if pass_obj:
            value = callback(obj, *cargs, *args, **ckwargs, **kwargs)
        else:
            value = callback(*cargs, *args, **ckwargs, **kwargs)
        if storage:
            storage[obj] = callback, limit - 1, cargs, ckwargs, pass_obj
        return value, limit - 1

    def emit(self, event: int | str, *args, **kwargs):
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

            for obj, quad in storage[event].items():
                new_limit = self.run_method(storage[event], obj, quad, *args, **kwargs)[1]
                if new_limit == 0:
                    obj.ignore(event)

    def ignore(self, event: int | str):
        """
        Ignore an event from the parent cluster.
        :param event: The event to ignore.
        :return: nothing
        """

        with self.action_lock as lock:
            storage = self.listener_storage
            if event not in storage or self not in storage[event]:
                return

            lock.delitem(storage[event], self)

    # Managing calculations
    @property
    def math_storage(self) -> dict[str, CallbackDict]:
        """
        Gets the listener storage for the parent cluster.
        :return: The listener storage.
        """

        parent = self.parent_cluster
        if parent is self:
            if self._mt_storage is None:
                self._mt_storage = {}
            return self._mt_storage
        return parent.math_storage

    def register_math(
        self, target: int | str, callback: callable, *args, limit: int = -1, pass_object: bool = False, **kwargs
    ):
        """
        Listen to a math recalculation target from the parent cluster.
        :param target: The target to listen to.
        :param callback: The callback to call when the event is emitted.
        :param args: The args to pass to the callback.
        :param limit: The number of times to call the callback. -1 for infinite.
        :param pass_object: Whether to pass the object owner to the callback. Has to be True when using the decorator.
        :param kwargs: The kwargs to pass to the callback.
        :return: nothing
        """

        with self.action_lock as lock:
            storage = self.math_storage
            if target not in storage:
                storage[target] = {}
            lock.setitem(storage[target], self, (callback, limit, args, kwargs, pass_object))

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

            for obj, quad in storage[target].items():
                current_value, new_limit = self.run_method(
                    storage[target], obj, quad, current_value, init_value=init_value, **kwargs
                )
                if new_limit == 0:
                    obj.ignore_math(target)

        return current_value

    def ignore_math(self, target: int | str):
        """
        Ignore a math target from the parent cluster.
        :param target: The target to ignore.
        :return: nothing
        """

        with self.action_lock as lock:
            storage = self.math_storage
            if target not in storage or self not in storage[target]:
                return

            lock.delitem(storage[target], self)

    # Bulk Cleanup methods
    def ignore_all(self):
        """
        Ignore all events and math targets.
        :return: nothing
        """

        with self.action_lock as lock:
            storage = self.listener_storage
            for event in storage:
                lock.delitem(storage[event], self)

            storage = self.math_storage
            for target in storage:
                lock.delitem(storage[target], self)

            for method in self.registry.replaced_methods:
                lock.delitem(self.registry.replaced_methods[method], self)

    def cleanup(self):
        """
        Cleanup the object.
        :return: nothing
        """

        self.ignore_all()
        for child in self.children.values():
            child.cleanup()
        self.children = {}

    # Replacement methods
    def run_replace(self, name: int | str, cb: CallbackDefinition, *args, **kwargs):
        value, limit = self.run_method(None, self, cb, *args, **kwargs)
        registry = self.registry
        repl = list(registry.replaced_methods[name][self])
        repl[2] = limit
        registry.replaced_methods[name][self] = tuple(repl)
        if not limit:
            with self.action_lock as lock:
                lock.delitem(registry.replaced_methods[name], self)
        return value

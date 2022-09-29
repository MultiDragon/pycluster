import abc

from pycluster.messenger.message_object import MessageObject
from pycluster.messenger.object_registry import ObjectRegistry


class MessageCluster(MessageObject, abc.ABC):
    object_type: int = 0

    def __init__(self, registry: ObjectRegistry):
        super().__init__()
        self._registry = registry

    @property
    def registry(self) -> ObjectRegistry:
        return self._registry

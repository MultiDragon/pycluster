from pycluster.messenger.cluster import MessageCluster
from pycluster.messenger.helpers import listen, math, replace, replaceable
from pycluster.messenger.message_object import MessageObject
from pycluster.messenger.object_registry import ObjectRegistry

registry = ObjectRegistry("priority", MessageCluster)


@registry.register(1)
class ObjectA(MessageObject):
    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self.value = 0

    @listen("test")
    def set_test(self, value):
        self.value = value
        self.parent_cluster.value = value

    @listen("should_be_later")
    def call_later(self, value):
        assert self.value == value

    @math("damage")
    def get_damage(self, value, **kwargs):
        return value + 5

    @replace("test_method")
    def test_method(self):
        raise RuntimeError("This should not be called")


@registry.register(2)
class ObjectB(MessageObject):
    @listen("test", priority=-1)
    def set_test(self, value):
        self.emit("should_be_later", value)

    @math("damage", priority=1)
    def get_damage_debug(self, value, **kwargs):
        return 1000

    @replace("test_method", priority=1)
    def test_method(self):
        return 5


@registry.register(3)
class ObjectC(MessageObject):
    @replaceable("test_method")
    def test_method(self):
        return 10


def construct_tree(order):
    cluster = MessageCluster(registry)
    registry.create_and_insert(1 + order, cluster, "child1")
    registry.create_and_insert(2 - order, cluster, "child2")
    object_c = registry.create_and_insert(3, cluster, "child3", cast_to=ObjectC)
    return cluster, object_c


def test_priority():
    for order in (0, 1):
        tree, object_c = construct_tree(order)
        assert tree.calculate("damage", 5) == 1000
        assert not hasattr(tree, "value")
        tree.emit("test", 15)
        assert tree.value == 15
        assert object_c.test_method() == 5
        tree.cleanup()
        assert tree.calculate("damage", 5) == 5


if __name__ == "__main__":
    test_priority()

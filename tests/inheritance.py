from pycluster.messenger.cluster import MessageCluster
from pycluster.messenger.helpers import listen, post_init
from pycluster.messenger.message_object import MessageObject
from pycluster.messenger.object_registry import ObjectRegistry

registry = ObjectRegistry("inheritance", MessageCluster)


@registry.register(1)
class BaseEventObject(MessageObject):
    def __init__(self, parent):
        super().__init__(parent)
        self.data = 0
        self.second_data = 0

    @listen("hello")
    def hello(self):
        self.data += 1

    @listen("magic")
    def magic(self):
        self.second_data += 1


@registry.register(2)
class InheritingEventObject(BaseEventObject):
    @listen("hello")
    def hello(self):
        super().hello()
        self.data += 1

    @post_init()
    def post_init(self):
        self.ignore("magic")


@registry.register(3)
class InheritingEventObject2(BaseEventObject):
    pass


def construct_tree():
    cluster = MessageCluster(registry)
    child1 = registry.create_and_insert(2, cluster, "child", cast_to=InheritingEventObject)
    child2 = registry.create_and_insert(1, cluster, "child2", cast_to=BaseEventObject)
    child3 = registry.create_and_insert(3, cluster, "child3", cast_to=InheritingEventObject2)
    return cluster, child1, child2, child3


def test_inheritance():
    tree, child1, child2, child3 = construct_tree()
    tree.emit("hello")
    tree.emit("magic")
    assert child1.data == 2 and child2.data == 1 and child3.data == 1
    assert child1.second_data == 0 and child2.second_data == 1 and child3.second_data == 1


if __name__ == "__main__":
    test_inheritance()

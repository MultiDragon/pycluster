from pycluster.messenger.cluster import MessageCluster
from pycluster.messenger.helpers import listen
from pycluster.messenger.message_object import MessageObject
from pycluster.messenger.object_registry import ObjectRegistry

registry = ObjectRegistry("copy", MessageCluster)


@registry.register(1)
class TestObject(MessageObject):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.data = 0

    @property
    def datagram(self):
        return self.data

    @datagram.setter
    def datagram(self, value):
        self.data = value

    @listen("hello")
    def hello(self):
        self.data += 1


def construct_tree():
    cluster = MessageCluster(registry)
    child1 = registry.create_and_insert(1, cluster, "child", cast_to=TestObject)
    return cluster, child1


def test_copies():
    tree, child1 = construct_tree()
    assert child1.data == 0
    tree.emit("hello")
    tree.emit("hello")
    tree.emit("hello")
    assert child1.data == 3
    cp = child1.copy()
    assert cp.parent_cluster is not tree.parent_cluster
    assert cp.data == 3
    tree.emit("hello")
    assert child1.data == 4
    assert cp.data == 3


if __name__ == "__main__":
    test_copies()

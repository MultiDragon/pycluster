from pycluster.messenger.cluster import MessageCluster
from pycluster.messenger.message_object import MessageObject
from pycluster.messenger.object_registry import ObjectRegistry

registry = ObjectRegistry("wrapping", MessageCluster)


@registry.register(1)
class TestObject(MessageObject):
    def __init__(self, parent, value=123):
        super().__init__(parent)
        self.value = value

    @property
    def datagram(self):
        return self.value

    @datagram.setter
    def datagram(self, value):
        self.value = value


@registry.register(2)
class TestSubcluster(MessageObject):
    pass


def construct_tree():
    cluster = MessageCluster(registry)
    child1 = registry.create_and_insert(1, cluster, "child1")
    child2 = registry.create_and_insert(1, cluster, "child2")
    internal_child = registry.create_and_insert(1, child2, "internal_child", value=456)

    return cluster.wrap()


def construct_leveled_tree():
    cluster = MessageCluster(registry)
    child1 = registry.create_and_insert(1, cluster, "child1")
    child2 = registry.create_and_insert(1, cluster, "child2")
    internal_child = registry.create_and_insert(1, child2, "internal_child", value=456)
    scluster = registry.create_and_insert(2, child2, "subcluster")
    scluster_child = registry.create_and_insert(1, scluster, "subcluster_child", value=789)
    scluster.add_child("child1", child1.copy_inplace())
    scluster.add_child("child2", child2.copy_inplace())

    return cluster.wrap()


def test_wrapping(wrapped):
    # print(wrapped)
    cluster = registry.unwrap(wrapped)
    assert cluster["child1"].value == 123
    assert cluster["child2"]["internal_child"].value == 456
    assert cluster.wrap() == wrapped


def wrapping_test():
    test_wrapping(construct_tree())
    leveled_tree = construct_leveled_tree()
    test_wrapping(leveled_tree)
    subcluster = leveled_tree[2]["child2"][2]["subcluster"]
    assert subcluster[2]["subcluster_child"][1] == 789
    test_wrapping(subcluster)


if __name__ == "__main__":
    wrapping_test()

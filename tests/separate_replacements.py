from pycluster.messenger.cluster import MessageCluster
from pycluster.messenger.helpers import replace
from pycluster.messenger.message_object import MessageObject
from pycluster.messenger.object_registry import ObjectRegistry

registry = ObjectRegistry("fizzle", MessageCluster)


@registry.register(1)
class ReplaceTarget(MessageObject):
    @registry.replaceable("test")
    def test(self, value):
        return 1 + value


@registry.register(2)
class ReplaceProxy(MessageObject):
    @replace("test")
    def test(self, value=0):
        return 2 + value


def construct_tree():
    cluster = MessageCluster(registry)
    child1 = registry.create_and_insert(1, cluster, "child", cast_to=ReplaceTarget)
    child2 = registry.create_and_insert(2, cluster, "child2", cast_to=ReplaceProxy)
    cluster2 = MessageCluster(registry)
    child3 = registry.create_and_insert(1, cluster2, "child", cast_to=ReplaceTarget)
    return child1, child3, child2


def test_separation():
    tree, tree2, proxy = construct_tree()
    assert tree.test(5) == 7
    assert tree2.test(5) == 6
    proxy.cleanup()
    assert tree.test(5) == 6


if __name__ == "__main__":
    test_separation()

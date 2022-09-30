from pycluster.messenger.cluster import MessageCluster
from pycluster.messenger.helpers import replace
from pycluster.messenger.message_object import MessageObject
from pycluster.messenger.object_registry import ObjectRegistry

registry = ObjectRegistry("replace", MessageCluster)


@registry.register(1)
class ReplaceTarget(MessageObject):
    @registry.replaceable("test")
    def test(self):
        return 1


@registry.register(2)
class ReplaceProxy(MessageObject):
    @replace("test")
    def test(self, replace_target):
        assert isinstance(replace_target, ReplaceTarget)
        return 2


def construct_tree():
    cluster = MessageCluster(registry)
    child1 = registry.create_and_insert(1, cluster, "child", cast_to=ReplaceTarget)
    child2 = registry.create_and_insert(2, cluster, "child2", cast_to=ReplaceProxy)
    return cluster, child1, child2


def test_replace():
    tree, child1, child2 = construct_tree()
    assert child1.test() == 2
    child2.cleanup()
    assert child1.test() == 1


if __name__ == "__main__":
    test_replace()

from pycluster.messenger.cluster import MessageCluster
from pycluster.messenger.helpers import replace
from pycluster.messenger.message_object import MessageObject
from pycluster.messenger.object_registry import FizzleReplace, ObjectRegistry

registry = ObjectRegistry("replace", MessageCluster)


@registry.register(1)
class ReplaceTarget(MessageObject):
    @registry.replaceable("test")
    def test(self, value):
        return 1 + value


@registry.register(2)
class ReplaceProxy(MessageObject):
    def __init__(self, parent, ignored_value: int, **kwargs):
        super().__init__(parent, **kwargs)
        self.ignored_value = ignored_value
        self.new_value = -1

    @replace("test")
    def test(self, replace_target, value):
        assert isinstance(replace_target, ReplaceTarget)
        if value == self.ignored_value:
            return FizzleReplace
        self.new_value = value
        return 2 + value


@registry.register(3)
class FinalReplaceProxy(MessageObject):
    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self.called = False

    @replace("test")
    def test(self, replace_target, value):
        assert value == 6
        self.called = True
        return FizzleReplace


def construct_tree():
    cluster = MessageCluster(registry)
    child1 = registry.create_and_insert(1, cluster, "child", cast_to=ReplaceTarget)
    child2 = registry.create_and_insert(2, cluster, "child2", cast_to=ReplaceProxy, ignored_value=6)
    child3 = registry.create_and_insert(3, cluster, "child3", cast_to=FinalReplaceProxy, ignored_value=6)
    return cluster, child1, child2, child3


def test_fizzle():
    tree, child1, child2, child3 = construct_tree()
    assert child2.new_value == -1
    assert child1.test(5) == 7
    assert child2.new_value == 5
    assert not child3.called
    assert child1.test(6) == 7
    assert child2.new_value == 5
    assert child3.called
    assert child1.test(3) == 5
    assert child2.new_value == 3


if __name__ == "__main__":
    test_fizzle()

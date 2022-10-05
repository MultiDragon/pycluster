from pycluster.messenger.cluster import MessageCluster
from pycluster.messenger.helpers import listen, math
from pycluster.messenger.message_object import MessageObject
from pycluster.messenger.object_registry import ObjectRegistry

registry = ObjectRegistry("calculations", MessageCluster)


@registry.register(1)
class CalculationObject(MessageObject):
    @listen("use_math")
    def use_math(self):
        self.register_math("damage", self.calculate_damage, limit=1, multiplier=2)

    @math("accuracy")
    def calculate_accuracy(self, value: float, **kwargs) -> float:
        return value * 1.5

    @math("use_init_value")
    def calculate_use_init_value(self, value: float, init_value: float, **kwargs) -> float:
        return value * init_value

    def calculate_damage(self, value: float, multiplier=1, **kwargs) -> float:
        return value * multiplier


@registry.register(2)
class TempObject(MessageObject):
    @math("magic")
    def change_magic(self, value: int, **kwargs) -> int:
        return value * 2


def construct_tree():
    cluster = MessageCluster(registry)
    child1 = registry.create_and_insert(1, cluster, "child", cast_to=CalculationObject)
    return cluster, child1


def test_calculation():
    tree, child1 = construct_tree()
    assert tree.calculate("accuracy", 1) == 1.5
    assert tree.calculate("damage", 1) == 1
    tree.emit("use_math")
    assert tree.calculate("damage", 1) == 2
    assert tree.calculate("use_init_value", 20) == 400
    assert tree.calculate("damage", 1) == 1
    child1.cleanup()
    assert tree.calculate("use_init_value", 20) == 20

    registry.create_and_insert(1, tree, "child2", cast_to=CalculationObject)
    registry.create_and_insert(1, tree, "child3", cast_to=CalculationObject)
    assert tree.calculate("use_init_value", 20) == 8000

    with registry.temporary_object(2, tree, cast_to=TempObject) as obj:
        assert isinstance(obj, TempObject)
        assert len(tree.children) == 4
        assert tree.calculate("magic", 10) == 20

    assert len(tree.children) == 3
    assert tree.calculate("magic", 10) == 10


if __name__ == "__main__":
    test_calculation()

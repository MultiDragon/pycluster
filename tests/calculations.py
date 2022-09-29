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


if __name__ == "__main__":
    test_calculation()

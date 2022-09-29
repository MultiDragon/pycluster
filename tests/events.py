from pycluster.messenger.cluster import MessageCluster
from pycluster.messenger.helpers import listen
from pycluster.messenger.message_object import MessageObject
from pycluster.messenger.object_registry import ObjectRegistry

registry = ObjectRegistry("events", MessageCluster)


@registry.register(1)
class EventObject(MessageObject):
    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self.data = 1
        self.hello_done = 0
        self.magic_done = 0
        self.advanced_done = 0

        self.x = self.y = self.test = None

    @listen("hello")
    def hello(self):
        # print("Hello!")
        self.hello_done += 1
        self.listen_to("advanced", self.advanced)

    def advanced(self):
        # print("Advanced triggered now")
        self.advanced_done = 1

    @listen("magic", limit=1)
    def magic(self):
        # print("Magic!")
        self.magic_done += 1

    @listen("set_data")
    def set_data(self, data):
        # print(f"Setting data to {data}")
        self.data = data
        if data == 4:
            self.ignore("hello")

    @listen("with_args", 23, test=567)
    def with_args(self, x, y, test):
        self.x, self.y, self.test = x, y, test


def construct_tree():
    cluster = MessageCluster(registry)
    child1 = registry.create_and_insert(1, cluster, "child", cast_to=EventObject)
    return cluster, child1


def test_events():
    tree, child1 = construct_tree()
    assert child1.advanced_done == 0
    tree.emit("advanced")
    assert child1.advanced_done == 0

    assert child1.data == 1 and child1.hello_done == 0 and child1.magic_done == 0
    tree.emit("hello")
    assert child1.data == 1 and child1.hello_done == 1 and child1.magic_done == 0
    tree.emit("magic")
    assert child1.data == 1 and child1.hello_done == 1 and child1.magic_done == 1
    tree.emit("magic")
    assert child1.data == 1 and child1.hello_done == 1 and child1.magic_done == 1
    tree.emit("set_data", 2)
    assert child1.data == 2 and child1.hello_done == 1 and child1.magic_done == 1
    tree.emit("hello")
    assert child1.data == 2 and child1.hello_done == 2 and child1.magic_done == 1
    tree.emit("set_data", 4)
    assert child1.data == 4 and child1.hello_done == 2 and child1.magic_done == 1
    tree.emit("hello")
    assert child1.data == 4 and child1.hello_done == 2 and child1.magic_done == 1
    child1.emit("with_args", y=34)
    assert child1.x == 23 and child1.y == 34 and child1.test == 567

    assert child1.advanced_done == 0
    tree.emit("advanced")
    assert child1.advanced_done == 1


if __name__ == "__main__":
    test_events()

# pyCluster

`pyCluster` provides a tree-like structure for storing objects, quickly wrapping and unwrapping
them into Python primitives, and a simple interface for subscribing to various events within a
cluster. The elements of a tree can subscribe to events sent from the same tree, as well as
mathematical requests.

## Usage

* Each element of the tree is a `MessageObject` object.
* Each element can use `listen`, `register_math` or `register_replace` to replace various
types of events: `listen` for events, `register_math` for mathematical requests, and
`register_replace` for replacing the body of a function with another function.
  * `register_replace` is currently not implemented, but works with decorators.
* Decorators `@listen`, `@math`, and `@replace` can be used to register functions to events
all the time while the object is alive.
* The object can be destroyed using `object.cleanup()`, which destroys its children as well.
* The object can be wrapped into a Python primitive using `object.wrap()`, which returns a
tuple that can be used to reconstruct the object (using `registry.unwrap()`).
* Any object can be copied using `object.copy()`, which returns a copy of the object.
  * As this copy will reside in a different cluster, this does not affect the cluster
  the original object lived in.
  * `copy_inplace` will copy the object into the current cluster. It will set its parent,
  but will NOT attach it properly. Use at your own risk when copying and unwrapping a cluster.
    (Will be fixed later)
* More examples in `tests/`.

## Requirements

* Python 3.9 or higher

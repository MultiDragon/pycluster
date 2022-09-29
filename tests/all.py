from calculations import test_calculation
from replace import test_replace
from wrapping import wrapping_test
from events import test_events
from inheritance import test_inheritance
from copy import test_copies


if __name__ == "__main__":
    print("Testing wrapping functions")
    wrapping_test()

    print("Testing events")
    test_events()

    print("Testing math")
    test_calculation()

    print("Testing inheritance")
    test_inheritance()

    print("Testing replacements")
    test_replace()

    print("Testing object copying")
    test_copies()

    print("All tests passed!")

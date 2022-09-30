import glob
import os


if __name__ == "__main__":
    failed_tests = []
    pwd = os.path.dirname(os.path.abspath(__file__))
    files = glob.glob(f"{pwd}/*.py")
    for i in files:
        if i.endswith(os.path.basename(__file__)):
            continue

        test_name = os.path.basename(i).replace(".py", "")
        print(f"Running test {test_name}")
        return_code = os.system(f"python3 {i}")
        if return_code != 0:
            print(f"Test {test_name} failed!")
            failed_tests.append(test_name)

    if failed_tests:
        print(f"Failed tests: {failed_tests}")
        exit(1)
    else:
        print("All tests passed!")
        exit(0)

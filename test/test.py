import io
import sys
import traceback
from contextlib import redirect_stdout
from dataclasses import dataclass

# Import these for side effect
import test_basic
import test_hash
import test_set
import test_sorted_set
from test_util import Server, all_tests


@dataclass
class TestResult:
    name: str
    passed: bool
    test_stdout: str
    server_stdout: str
    server_stderr: str
    exc: Exception | None

    def print(self):
        status = "PASS" if self.passed else "FAIL"
        print(f"{self.name}... {status}")
        print()
        print("server stdout:")
        print(self.server_stdout)
        print()
        print("server stderr:")
        print(self.server_stderr)
        print()
        print("test stdout:")
        print(self.test_stdout)
        if self.exc is not None:
            print()
            print("exception:")
            traceback.print_exception(self.exc)


def main():
    passed: list[TestResult] = []
    failed: list[TestResult] = []
    for test_fn in all_tests:
        print(test_fn.__name__, end="... ", flush=True)
        server = None
        with io.StringIO() as saved_output:
            exc = None
            try:
                with redirect_stdout(saved_output):
                    with Server() as server:
                        test_fn(server)
                success = True
                print("PASS")
            except Exception as e:
                exc = e
                success = False
                print("FAIL")
                traceback.print_exc()

            if server is not None:
                s_out, s_err = server.get_output()
            else:
                s_out, s_err = "", ""

            result = TestResult(
                name=test_fn.__name__,
                passed=success,
                test_stdout=saved_output.getvalue(),
                server_stdout=s_out,
                server_stderr=s_err,
                exc=exc,
            )
            if success:
                passed.append(result)
            else:
                failed.append(result)

    print()
    print("Summary")
    print(f"Passed: {len(passed)}")
    print(f"Failed: {len(failed)}")

    if len(failed) == 0:
        sys.exit(0)
    else:
        print()
        for res in failed:
            res.print()
        sys.exit(1)


if __name__ == "__main__":
    main()

import io
import itertools
import subprocess
import sys
import tempfile
import traceback
import typing
from collections.abc import Iterator
from contextlib import redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType

from client import Client, ReqType, RespObject, RespType

root_dir = Path(__file__).parent.parent
run_command = (root_dir / "bin" / "server",)


class ServerProc:
    process: subprocess.Popen[str]
    stdout_data: str | None = None
    stderr_data: str | None = None

    def __init__(self):
        self.process = subprocess.Popen(
            run_command,
            cwd=root_dir,
            stdin=subprocess.DEVNULL,
            # Use temp files to capture output as pipes can fill up if there is too
            # much output
            # TODO: Is there a better way to capture output?
            stdout=tempfile.TemporaryFile(),
            stderr=tempfile.TemporaryFile(),
            text=True,
        )


def read_all_and_close(file: typing.IO[bytes]) -> str:
    try:
        with file:
            _ = file.seek(0)
            return b"".join(file.readlines()).decode()
    except IOError:
        return ""


class Server:
    process: subprocess.Popen[str] | None = None
    stdout_file: typing.IO[bytes]
    stdout_data: str | None = None
    stderr_file: typing.IO[bytes]
    stderr_data: str | None = None

    def __init__(self):
        self.stdout_file = tempfile.TemporaryFile()
        self.stderr_file = tempfile.TemporaryFile()

        self.process = subprocess.Popen(
            run_command,
            cwd=root_dir,
            stdin=subprocess.DEVNULL,
            # Use temp files to capture output as pipes can fill up if there is too
            # much output
            # TODO: Is there a better way to capture output?
            stdout=self.stdout_file,
            stderr=self.stderr_file,
            text=True,
        )

    def close(self):
        if self.process is None:
            return
        self.process.kill()
        _ = self.process.wait()

    def make_client(self) -> Client:
        return Client(timeout=5)

    def __enter__(self):
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ):
        assert self.process is not None
        self.close()

        self.stdout_data = read_all_and_close(self.stdout_file)
        self.stderr_data = read_all_and_close(self.stderr_file)

    def get_output(self) -> tuple[str, str]:
        if self.process is None:
            raise Exception("Server not started")
        if self.process.poll() is None:
            raise Exception("Server not finished")

        assert self.stdout_data is not None
        assert self.stderr_data is not None

        return self.stdout_data, self.stderr_data


TestFn = typing.Callable[[Server], None]
all_tests: list[TestFn] = []


def test(test_fn: TestFn) -> TestFn:
    """Annotation for marking tests."""
    all_tests.append(test_fn)
    return test_fn


@test
def test_keys_empty_on_startup(server: Server):
    c = server.make_client()
    res, val = c.send(ReqType.KEYS)
    assert res == RespType.OK
    assert val == []


@test
def test_get_not_exist_returns_error(server: Server):
    c = server.make_client()
    res, val = c.send(ReqType.GET, "abc")
    assert res == RespType.OK
    assert val is None


@test
def test_get_returns_set_int_value(server: Server):
    c = server.make_client()
    res, val = c.send(ReqType.SET, "abc", 42)
    assert res == RespType.OK
    res, val = c.send(ReqType.GET, "abc")
    assert res == RespType.OK
    assert val == 42


@test
def test_get_returns_last_set_value(server: Server):
    c = server.make_client()
    res, val = c.send(ReqType.SET, "abc", 42)
    assert res == RespType.OK
    res, val = c.send(ReqType.SET, "abc", "def")
    assert res == RespType.OK
    res, val = c.send(ReqType.GET, "abc")
    assert res == RespType.OK
    assert val == b"def"


@test
def test_del_returns_false_if_not_present(server: Server):
    c = server.make_client()
    res, val = c.send(ReqType.DEL, "abc")
    assert res == RespType.OK
    assert val is False


@test
def test_del_returns_true_if_present(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.SET, "abc", "def")
    assert res == RespType.OK
    res, val = c.send(ReqType.DEL, "abc")
    assert res == RespType.OK
    assert val is True


@test
def test_get_after_del_returns_err(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.SET, "abc", "def")
    assert res == RespType.OK
    res, _val = c.send(ReqType.DEL, "abc")
    assert res == RespType.OK
    res, val = c.send(ReqType.GET, "abc")
    assert res == RespType.OK
    assert val is None


@test
def test_get_after_set_after_del_returns_value(server: Server):
    c = server.make_client()
    res, val = c.send(ReqType.SET, "abc", "def")
    assert res == RespType.OK
    res, val = c.send(ReqType.DEL, "abc")
    assert res == RespType.OK
    res, val = c.send(ReqType.SET, "abc", "new value")
    assert res == RespType.OK
    res, val = c.send(ReqType.GET, "abc")
    assert res == RespType.OK
    assert val == b"new value"


@test
def test_pipeline_set_get_commands(server: Server):
    c = server.make_client()
    c.send_req(ReqType.SET, "abc", "value")
    c.send_req(ReqType.GET, "abc")

    res, val = c.recv_resp()
    assert res == RespType.OK
    res, val = c.recv_resp()
    assert res == RespType.OK
    assert val == b"value"


@test
def test_set_and_get_10_000_keys(server: Server):
    n = 10_000
    c = server.make_client()

    # Pipeline to speed things up
    for i in range(n):
        c.send_req(ReqType.SET, f"key:{i}", f"value:{i}")
    for i in range(n):
        res, val = c.recv_resp()
        assert res == RespType.OK

    for i in range(n):
        c.send_req(ReqType.GET, f"key:{i}")
    for i in range(n):
        res, val = c.recv_resp()
        assert res == RespType.OK
        assert val == f"value:{i}".encode("ascii")


# Hash map tests


@test
def test_hget_returns_value_after_hset(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.HSET, "map", "field", "value")
    assert res == RespType.OK
    res, val = c.send(ReqType.HGET, "map", "field")
    assert res == RespType.OK
    assert val == b"value"


@test
def test_hget_returns_value_after_latest_hset(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.HSET, "map", "field", "old-val")
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "field", "new-val")
    assert res == RespType.OK

    res, val = c.send(ReqType.HGET, "map", "field")
    assert res == RespType.OK
    assert val == b"new-val"


@test
def test_hget_missing_key(server: Server):
    c = server.make_client()
    res, val = c.send(ReqType.HGET, "map", "field")
    assert res == RespType.OK
    assert val is None


@test
def test_hget_missing_field(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.HSET, "map", "field1", "value")
    assert res == RespType.OK
    res, val = c.send(ReqType.HGET, "map", "field2")
    assert res == RespType.OK
    assert val is None


@test
def test_hlen_0_if_not_found(server: Server):
    c = server.make_client()
    res, val = c.send(ReqType.HLEN, "map")
    assert res == RespType.OK
    assert val == 0


@test
def test_hlen_err_if_not_hash(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.SET, "map", "projection")
    assert res == RespType.OK
    res, _val = c.send(ReqType.HLEN, "map")
    assert res == RespType.ERR


@test
def test_hlen_counts_unique_set_keys(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.HSET, "map", "pigs", 1)
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "cows", "nope")
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "farmers", 2)
    assert res == RespType.OK
    # Update shouldn't increase len
    res, _val = c.send(ReqType.HSET, "map", "pigs", 3)
    assert res == RespType.OK

    res, val = c.send(ReqType.HLEN, "map")
    assert res == RespType.OK
    assert val == 3


@test
def test_hlen_decreases_after_hdel(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.HSET, "map", "pigs", 1)
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "cows", "nope")
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "farmers", 2)
    assert res == RespType.OK
    # Update shouldn't increase len
    res, _val = c.send(ReqType.HDEL, "map", "pigs")
    assert res == RespType.OK

    res, val = c.send(ReqType.HLEN, "map")
    assert res == RespType.OK
    assert val == 2


@test
def test_hkeys_empty_if_not_found(server: Server):
    c = server.make_client()
    res, val = c.send(ReqType.HKEYS, "map")
    assert res == RespType.OK
    assert val == []


@test
def test_hkeys_err_if_not_hash(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.SET, "map", "projection")
    assert res == RespType.OK
    res, _val = c.send(ReqType.HKEYS, "map")
    assert res == RespType.ERR


@test
def test_hkeys_counts_unique_set_keys(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.HSET, "map", "pigs", 1)
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "cows", "nope")
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "farmers", 2)
    assert res == RespType.OK
    # Update shouldn't increase len
    res, _val = c.send(ReqType.HSET, "map", "pigs", 3)
    assert res == RespType.OK

    res, val = c.send(ReqType.HKEYS, "map")
    assert res == RespType.OK
    assert isinstance(val, list)
    assert set(val) == {b"pigs", b"cows", b"farmers"}


@test
def test_hkeys_decreases_after_hdel(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.HSET, "map", "pigs", 1)
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "cows", "nope")
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "farmers", 2)
    assert res == RespType.OK
    # Update shouldn't increase len
    res, _val = c.send(ReqType.HDEL, "map", "pigs")
    assert res == RespType.OK

    res, val = c.send(ReqType.HKEYS, "map")
    assert res == RespType.OK
    assert isinstance(val, list)
    assert set(val) == {b"cows", b"farmers"}


@test
def test_hgetall_0_if_not_found(server: Server):
    c = server.make_client()
    res, val = c.send(ReqType.HGETALL, "map")
    assert res == RespType.OK
    assert val == []


@test
def test_hgetall_err_if_not_hash(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.SET, "map", "projection")
    assert res == RespType.OK
    res, _val = c.send(ReqType.HGETALL, "map")
    assert res == RespType.ERR


def resp_pairs(val: RespObject) -> Iterator[tuple[RespObject, RespObject]]:
    assert isinstance(val, list)
    for group in itertools.batched(val, 2):
        assert len(group) == 2, "Expected list to be of even length"
        yield (group[0], group[1])


def resp_dict(val: RespObject) -> dict[RespObject, RespObject]:
    mapping: dict[RespObject, RespObject] = {}
    for k, v in resp_pairs(val):
        assert k not in mapping, "Expected unique keys"
        mapping[k] = v
    return mapping


@test
def test_hgetall_has_unique_set_keys(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.HSET, "map", "pigs", 1)
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "cows", "nope")
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "farmers", 2)
    assert res == RespType.OK
    # Update shouldn't increase len
    res, _val = c.send(ReqType.HSET, "map", "pigs", 3)
    assert res == RespType.OK

    res, val = c.send(ReqType.HGETALL, "map")
    assert res == RespType.OK
    assert isinstance(val, list)
    assert len(val) == 6
    assert resp_dict(val) == {b"pigs": 3, b"cows": b"nope", b"farmers": 2}


@test
def test_hgetall_decreases_after_hdel(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.HSET, "map", "pigs", 1)
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "cows", "nope")
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "farmers", 2)
    assert res == RespType.OK
    # Update shouldn't increase len
    res, _val = c.send(ReqType.HDEL, "map", "pigs")
    assert res == RespType.OK

    res, val = c.send(ReqType.HGETALL, "map")
    assert res == RespType.OK
    assert isinstance(val, list)
    assert len(val) == 4
    assert resp_dict(val) == {b"cows": b"nope", b"farmers": 2}


@test
def test_hset_and_hget_10_000_keys(server: Server):
    n = 10_000
    c = server.make_client()

    for i in range(n):
        c.send_req(ReqType.HSET, "hash", f"key:{i}", f"value:{i}")
    for i in range(n):
        res, val = c.recv_resp()
        assert res == RespType.OK

    for i in range(n):
        c.send_req(ReqType.HGET, "hash", f"key:{i}")
    for i in range(n):
        res, val = c.recv_resp()
        assert res == RespType.OK
        assert val == f"value:{i}".encode("ascii")


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

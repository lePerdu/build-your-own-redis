import io
import subprocess
import sys
import tempfile
import traceback

from contextlib import redirect_stdout
from dataclasses import dataclass
from pathlib import Path

from client import Client, Request, Response

root_dir = Path(__file__).parent.parent
run_command = (root_dir / 'bin' / 'server',)

all_tests = []


def test(test_fn):
    """Annotation for marking tests."""
    all_tests.append(test_fn)
    return test_fn


@test
def test_keys_empty_on_startup(server):
    c = server.make_client()
    res, val = c.send(Request.KEYS)
    assert res == Response.OK
    assert val == []


@test
def test_get_not_exist_returns_error(server):
    c = server.make_client()
    res, val = c.send(Request.GET, 'abc')
    assert res == Response.ERR


@test
def test_get_returns_set_int_value(server):
    c = server.make_client()
    res, val = c.send(Request.SET, 'abc', 42)
    assert res == Response.OK
    res, val = c.send(Request.GET, 'abc')
    assert res == Response.OK
    assert val == 42


@test
def test_get_returns_last_set_value(server):
    c = server.make_client()
    res, val = c.send(Request.SET, 'abc', 42)
    assert res == Response.OK
    res, val = c.send(Request.SET, 'abc', 'def')
    assert res == Response.OK
    res, val = c.send(Request.GET, 'abc')
    assert res == Response.OK
    assert val == b'def'


@test
def test_del_returns_err_if_not_present(server):
    c = server.make_client()
    res, val = c.send(Request.DEL, 'abc')
    assert res == Response.ERR


@test
def test_del_returns_ok_if_present(server):
    c = server.make_client()
    res, val = c.send(Request.SET, 'abc', 'def')
    assert res == Response.OK
    res, val = c.send(Request.DEL, 'abc')
    assert res == Response.OK


@test
def test_get_after_del_returns_err(server):
    c = server.make_client()
    res, val = c.send(Request.SET, 'abc', 'def')
    assert res == Response.OK
    res, val = c.send(Request.DEL, 'abc')
    assert res == Response.OK
    res, val = c.send(Request.GET, 'abc')
    assert res == Response.ERR


@test
def test_get_after_set_after_del_returns_value(server):
    c = server.make_client()
    res, val = c.send(Request.SET, 'abc', 'def')
    assert res == Response.OK
    res, val = c.send(Request.DEL, 'abc')
    assert res == Response.OK
    res, val = c.send(Request.SET, 'abc', 'new value')
    assert res == Response.OK
    res, val = c.send(Request.GET, 'abc')
    assert res == Response.OK
    assert val == b'new value'


@test
def test_pipeline_set_get_commands(server):
    c = server.make_client()
    c._send(Request.SET, 'abc', 'value')
    c._send(Request.GET, 'abc')

    res, val = c._recv()
    assert res == Response.OK
    res, val = c._recv()
    assert res == Response.OK
    assert val == b'value'

@test
def test_set_and_get_10_000_keys(server):
    n = 10_000
    c = server.make_client()
    for i in range(n):
        res, val = c.send(Request.SET, f'key:{i}', f'value:{i}')
        assert res == Response.OK

    for i in range(n):
        res, val = c.send(Request.GET, f'key:{i}')
        assert res == Response.OK
        assert val == f'value:{i}'.encode('ascii')


@dataclass
class TestResult:
    name: str
    passed: bool
    test_stdout: str
    server_stdout: str
    server_stderr: str
    exc: Exception | None

    def print(self):
        status = 'PASS' if self.passed else 'FAIL'
        print(f'{self.name}... {status}')
        print()
        print('server stdout:')
        print(self.server_stdout)
        print()
        print('server stderr:')
        print(self.server_stderr)
        print()
        print('test stdout:')
        print(self.test_stdout)
        if self.exc is not None:
            print()
            print('exception:')
            traceback.print_exception(self.exc)


class Server:
    def __init__(self):
        self.process = None
        self.stdout_file = None
        self.stdout_data = None
        self.stderr_file = None
        self.stderr_data = None

    def make_client(self):
        return Client(timeout=10)

    def start(self):
        # Use temp files to capture output as pipes can fill up if there is too
        # much output
        # TODO: Is there a better way to capture output?
        self.stdout_file = tempfile.TemporaryFile()
        self.stderr_file = tempfile.TemporaryFile()
        self.process = subprocess.Popen(
            run_command,
            cwd=root_dir,
            stdin=subprocess.DEVNULL,
            stdout=self.stdout_file,
            stderr=self.stderr_file,
            text=True,
        )

    def stop(self):
        self.process.kill()
        self.process.wait()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        if self.process is not None:
            self.stop()

            self.stdout_data = ''.join(self.stdout_file.readlines())
            self.stdout_file.close()

            self.stderr_data = ''.join(self.stderr_file.readlines())
            self.stderr_file.close()


    def get_output(self):
        if self.process is None:
            raise Exception('Server not started')
        if self.process.poll() is None:
            raise Exception('Server not finished')

        return self.stdout_data, self.stderr_data


def main():
    passed = []
    failed = []
    for test_fn in all_tests:
        print(test_fn.__name__, end='... ', flush=True)
        server = Server()
        with io.StringIO() as saved_output:
            exc = None
            try:
                with redirect_stdout(saved_output):
                    with server:
                        test_fn(server)
                success = True
                print('PASS')
            except Exception as e:
                exc = e
                success = False
                print('FAIL')
                traceback.print_exc()

            s_out, s_err = server.get_output()
            result = TestResult(
                name=test_fn.__name__,
                passed=success,
                test_stdout=saved_output.getvalue(),
                server_stdout=s_out,
                server_stderr=s_err,
                exc=exc
            )
            if success:
                passed.append(result)
            else:
                failed.append(result)

    print()
    print('Summary')
    print(f'Passed: {len(passed)}')
    print(f'Failed: {len(failed)}')

    if len(failed) == 0:
        sys.exit(0)
    else:
        print()
        for res in failed:
            res.print()
        sys.exit(1)


if __name__ == '__main__':
    main()

import subprocess
import tempfile
import typing
from pathlib import Path
from types import TracebackType

from client import Client, ProtocolError, ReqType

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

    def stop(self):
        if self.process is None:
            return
        if self.process.poll() is not None:
            return

        # Try to kill via client
        c = self.make_client()
        try:
            _ = c.send(ReqType.SHUTDOWN)
        except ProtocolError:
            pass

        try:
            return self.process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            self.process.kill()
            return self.process.wait()

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
        exit_code = self.stop()

        self.stdout_data = read_all_and_close(self.stdout_file)
        self.stderr_data = read_all_and_close(self.stderr_file)

        if exit_code != 0:
            raise Exception(f"Server didn't close gracefully: {exit_code}")

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

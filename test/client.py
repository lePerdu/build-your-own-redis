import enum
import itertools
import math
import re
import socket
import time
from collections.abc import Buffer, Iterator
from types import TracebackType


class RespType(enum.Enum):
    NULL = b"_"
    BOOLEAN = b"#"
    NUMBER = b":"
    DOUBLE = b","
    SIMPLE_STR = b"+"
    SIMPLE_ERR = b"-"
    BLOB_STR = b"$"
    ARRAY = b"*"

    @enum.property
    def byte(self) -> int:
        return self.value[0]


ReqObject = int | float | str | bytes
RespObject = None | int | float | bytes | list["RespObject"]


class ClientError(Exception):
    pass


class ProtocolError(ClientError):
    pass


class ParseError(ProtocolError):
    pass


class UnexpectedEofError(ProtocolError):
    pass


class ResponseError(ClientError):
    message: bytes

    def __init__(self, message: bytes):
        super().__init__(self, message.decode())
        self.message = message


class NotEnoughData(Exception):
    """Internal exception used to indicate more data is required.
    This is not intended to be raised from top-level methods."""

    pass


def resp_serialize_array_header(buffer: bytearray, arr_len: int):
    buffer.extend(RespType.ARRAY.value)
    buffer.extend(f"{arr_len}\r\n".encode())


def resp_serialize_object(buffer: bytearray, obj: ReqObject):
    if isinstance(obj, int | float | str):
        obj = f"{obj}".encode()
    buffer.extend(RespType.BLOB_STR.value)
    buffer.extend(f"{len(obj)}\r\n".encode())
    buffer.extend(obj)
    buffer.extend(b"\r\n")


CR = ord("\r")
LF = ord("\n")


def skip_crlr(buffer: memoryview) -> memoryview:
    if len(buffer) < 2:
        raise NotEnoughData

    if buffer[0] == CR and buffer[1] == LF:
        return buffer[2:]

    raise ParseError("expected <CR><LF>")


POSSIBLE_NUMBER_REGEX = re.compile(rb"[+-]?\d*")
POSSIBLE_DOUBLE_REGEX = re.compile(rb"[+-]?(\d+\.?\d*[eE]?[+-]?\d*)?|nan|-?inf")
SIMPLE_STR_REGEX = re.compile(b"[^\r\n]*")


def parse_number(buffer: memoryview) -> tuple[int, memoryview]:
    m = POSSIBLE_NUMBER_REGEX.match(buffer)
    if m is None:
        raise ParseError("invalid number")
    # Throws if <CR><LF> is invalid or more data is needed
    after_crlf = skip_crlr(buffer[len(m[0]) :])
    return int(m[0]), after_crlf


def parse_double(buffer: memoryview) -> tuple[float, memoryview]:
    m = POSSIBLE_DOUBLE_REGEX.match(buffer)
    if m is None:
        raise ParseError("invalid number")
    # Throws if <CR><LF> is invalid or more data is needed
    after_crlf = skip_crlr(buffer[len(m[0]) :])
    return float(m[0]), after_crlf


def parse_simple_str(buffer: memoryview) -> tuple[bytes, memoryview]:
    m = SIMPLE_STR_REGEX.match(buffer)
    if m is None:
        raise ParseError("invalid simple string")
    # Throws if <CR><LF> is invalid or more data is needed
    after_crlf = skip_crlr(buffer[len(m[0]) :])
    return m[0], after_crlf


def parse_blob_str(buffer: memoryview) -> tuple[bytes, memoryview]:
    str_len, buffer = parse_number(buffer)
    if str_len < 0:
        raise ParseError("invalid blob string length")
    if len(buffer) < str_len + 2:
        raise NotEnoughData
    return bytes(buffer[:str_len]), skip_crlr(buffer[str_len:])


def parse_array(buffer: Buffer) -> tuple[list[RespObject] | ResponseError, Buffer]:
    # TODO: Store partial array in parser so array can be read incrementally
    arr_len, buffer = parse_number(memoryview(buffer))
    if arr_len < 0:
        raise ParseError("invalid array length")

    arr: list[RespObject] = []
    for _ in range(arr_len):
        elem, buffer = try_parse_object(buffer)
        # TODO: How to handle this case?
        if isinstance(elem, ResponseError):
            return elem, buffer
        arr.append(elem)
    return arr, buffer


def try_parse_object(buffer: Buffer) -> tuple[RespObject | ResponseError, Buffer]:
    buffer = memoryview(buffer)
    if len(buffer) == 0:
        raise NotEnoughData

    type_byte = buffer[0]
    buffer = buffer[1:]
    match type_byte:
        case RespType.NULL.byte:
            return None, skip_crlr(buffer)
        case RespType.BOOLEAN.byte:
            if len(buffer) < 3:
                raise NotEnoughData
            if buffer[0] == ord("t"):
                val = True
            elif buffer[0] == ord("f"):
                val = False
            else:
                raise ParseError(f"expected `t' or `f' but got {buffer[0:1]}")

            return val, skip_crlr(buffer[1:])
        case RespType.NUMBER.byte:
            return parse_number(buffer)
        case RespType.DOUBLE.byte:
            return parse_double(buffer)
        case RespType.SIMPLE_STR.byte:
            return parse_simple_str(buffer)
        case RespType.SIMPLE_ERR.byte:
            msg, buffer = parse_simple_str(buffer)
            return ResponseError(msg), buffer
        case RespType.BLOB_STR.byte:
            return parse_blob_str(buffer)
        case RespType.ARRAY.byte:
            return parse_array(buffer)
        case _:
            raise ParseError(f"Invalid response type: {bytes((type_byte,))}")


def retry_for_connection(
    address: tuple[str | None, int],
    timeout: float | None,
    *,
    poll_interval: float = 0.01,
) -> socket.socket:
    """Wrapper around `socket.create_connection` which waits for the server to
    accept connections."""
    start_time = time.perf_counter()
    if timeout is not None:
        end_time = start_time + timeout
    else:
        end_time = math.inf
    while True:
        if timeout is not None:
            remaining = end_time - time.perf_counter()
            if remaining < 0:
                raise TimeoutError(
                    f"{address} not accepting connections after {timeout} seconds"
                )
        else:
            remaining = None

        try:
            return socket.create_connection(address, timeout=remaining)
        except ConnectionRefusedError:
            time.sleep(poll_interval)


class Client:
    conn: socket.socket
    recv_buf: bytearray
    recv_len: int

    def __init__(
        self, host: str = "127.0.0.1", port: int = 1234, *, timeout: float | None = None
    ):
        self.conn = retry_for_connection((host, port), timeout=timeout)
        self.recv_buf = bytearray(4096)
        self.recv_len = 0

    def send_req(self, *args: ReqObject):
        """Send a single request, but don't wait for the response."""
        buffer = bytearray()
        resp_serialize_array_header(buffer, len(args))
        for a in args:
            resp_serialize_object(buffer, a)

        print("sending", buffer)
        self.conn.sendall(buffer)

    def recv_resp(self) -> RespObject:
        """Receive a single response."""
        # TODO: Reduce the amount of copies
        while True:
            try:
                resp_obj, rest = try_parse_object(
                    memoryview(self.recv_buf)[: self.recv_len]
                )
                # Reset the buffers after a good parse
                self.recv_buf = bytearray(rest)
                self.recv_len = len(self.recv_buf)
                if isinstance(resp_obj, ResponseError):
                    raise resp_obj
                return resp_obj
            except NotEnoughData:
                pass

            if len(self.recv_buf) - self.recv_len < 1024:
                extra_cap = max(len(self.recv_buf), 1024)
                self.recv_buf.extend(bytes(extra_cap))

            chunk_len = self.conn.recv_into(memoryview(self.recv_buf)[self.recv_len :])
            print("received", self.recv_buf[self.recv_len : self.recv_len + chunk_len])
            if chunk_len == 0:
                raise UnexpectedEofError
            self.recv_len += chunk_len

    def send(self, *args: ReqObject) -> RespObject:
        """Send and receive a single response."""
        self.send_req(*args)
        return self.recv_resp()

    def send_shutdown(self):
        """This needs some special handling since the server won't send a response."""
        try:
            val = self.send(b"SHUTDOWN")
        except UnexpectedEofError:
            return
        raise ProtocolError("Unexpected response from SHUTDOWN command", val)

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ):
        self.close()


def resp_object_pairs(val: RespObject) -> Iterator[tuple[RespObject, RespObject]]:
    assert isinstance(val, list)
    for group in itertools.batched(val, 2):
        assert len(group) == 2, "Expected list to be of even length"
        yield (group[0], group[1])


def resp_object_dict(val: RespObject) -> dict[RespObject, RespObject]:
    mapping: dict[RespObject, RespObject] = {}
    for k, v in resp_object_pairs(val):
        assert k not in mapping, "Expected unique keys"
        mapping[k] = v
    return mapping

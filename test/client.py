import enum
import math
import socket
import time
from collections.abc import Buffer
from types import TracebackType


class ReqType(enum.IntEnum):
    GET = 0
    SET = 1
    DEL = 2
    KEYS = 3

    HGET = 16
    HSET = 17
    HDEL = 18
    HLEN = 19
    HKEYS = 20
    HGETALL = 21

    SADD = 32
    SISMEMBER = 33
    SREM = 34
    SCARD = 35
    SRANDMEMBER = 36
    SPOP = 37
    SMEMBERS = 38

    SHUTDOWN = 255


class RespType(enum.IntEnum):
    OK = 0
    ERR = 1


class ObjType(enum.IntEnum):
    NIL = 0
    TRUE = 1
    FALSE = 2
    INT = 3
    STR = 4
    ARR = 5


ReqObject = int | str | bytes
RespObject = None | int | bytes | list["RespObject"]

Response = tuple[RespType, RespObject]


class ProtocolError(Exception):
    pass


class ParseError(ProtocolError):
    pass


class UnexpectedEofError(ProtocolError):
    pass


class NotEnoughData(Exception):
    pass


def extend_with_arg(buffer: bytearray, arg: ReqObject):
    if isinstance(arg, int):
        buffer.append(ObjType.INT)
        buffer.extend(arg.to_bytes(8, "little"))
    elif isinstance(arg, str):
        buffer.append(ObjType.STR)
        # TODO: Encode as UTF-8?
        arg = arg.encode("ascii")
        buffer.extend(len(arg).to_bytes(4, "little"))
        buffer.extend(arg)
    else:
        assert isinstance(arg, bytes), f"Invalid request argument type: {type(arg)}"
        buffer.append(ObjType.STR)
        buffer.extend(len(arg).to_bytes(4, "little"))
        buffer.extend(arg)


def try_parse_object(buffer: Buffer) -> tuple[RespObject, Buffer]:
    buffer = memoryview(buffer)
    if len(buffer) == 0:
        raise NotEnoughData

    type_byte = buffer[0]
    buffer = buffer[1:]
    if type_byte == ObjType.NIL:
        return None, buffer
    elif type_byte == ObjType.TRUE:
        return True, buffer
    elif type_byte == ObjType.FALSE:
        return False, buffer
    elif type_byte == ObjType.INT:
        if len(buffer) < 8:
            raise NotEnoughData
        return int.from_bytes(buffer[:8], "little"), buffer[8:]
    elif type_byte == ObjType.STR:
        if len(buffer) < 4:
            raise NotEnoughData
        str_len = int.from_bytes(buffer[:4], "little")
        buffer = buffer[4:]
        if len(buffer) < str_len:
            raise NotEnoughData
        return bytes(buffer[:str_len]), buffer[str_len:]
    elif type_byte == ObjType.ARR:
        if len(buffer) < 4:
            raise NotEnoughData

        arr_len = int.from_bytes(buffer[:4], "little")
        buffer = buffer[4:]
        arr: list[RespObject] = []
        for _ in range(arr_len):
            elem, buffer = try_parse_object(buffer)
            arr.append(elem)
        return arr, buffer
    else:
        raise ParseError(f"Invalid response type: f{type_byte}")


def try_parse_response(buffer: Buffer) -> tuple[Response, Buffer]:
    buffer = memoryview(buffer)

    if len(buffer) == 0:
        raise NotEnoughData
    resp_code = RespType(buffer[0])
    obj, rest = try_parse_object(buffer[1:])
    return (resp_code, obj), rest


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

    def send_req(self, request: ReqType, *args: ReqObject):
        """Send a single request, but don't wait for the response."""
        buffer = bytearray()
        buffer.append(request)
        for a in args:
            extend_with_arg(buffer, a)

        print("sending", buffer)
        self.conn.sendall(buffer)

    def recv_resp(self) -> Response:
        """Receive a single response."""
        # TODO: Reduce the amount of copies
        while True:
            try:
                resp, rest = try_parse_response(
                    memoryview(self.recv_buf)[: self.recv_len]
                )
                # Reset the buffers after a good parse
                self.recv_buf = bytearray(rest)
                self.recv_len = len(self.recv_buf)
                return resp
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

    def send(self, request: ReqType, *args: ReqObject) -> Response:
        """Send and receive a single response."""
        self.send_req(request, *args)
        return self.recv_resp()

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

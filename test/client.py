import enum
import itertools
import math
import socket
import struct
import time
from collections.abc import Buffer, Iterator
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
    FLOAT = 4
    STR = 5
    ARR = 6


ReqObject = int | float | str | bytes
RespObject = None | int | float | bytes | list["RespObject"]

Response = tuple[RespType, RespObject]


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
        super().__init__(self)
        self.message = message


class NotEnoughData(Exception):
    """Internal exception used to indicate more data is required.
    This is not intended to be raised from top-level methods."""

    pass


def extend_with_arg(buffer: bytearray, arg: ReqObject):
    if isinstance(arg, int):
        buffer.append(ObjType.INT)
        buffer.extend(arg.to_bytes(8, "little"))
    elif isinstance(arg, float):
        buffer.append(ObjType.FLOAT)
        buffer.extend(struct.pack("d", arg))
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
    match type_byte:
        case ObjType.NIL:
            return None, buffer
        case ObjType.TRUE:
            return True, buffer
        case ObjType.FALSE:
            return False, buffer
        case ObjType.INT:
            if len(buffer) < 8:
                raise NotEnoughData
            return int.from_bytes(buffer[:8], "little"), buffer[8:]
        case ObjType.FLOAT:
            if len(buffer) < 8:
                raise NotEnoughData
            val: float = struct.unpack("d", buffer[:8])[0]
            return val, buffer[8:]
        case ObjType.STR:
            if len(buffer) < 4:
                raise NotEnoughData
            str_len = int.from_bytes(buffer[:4], "little")
            buffer = buffer[4:]
            if len(buffer) < str_len:
                raise NotEnoughData
            return bytes(buffer[:str_len]), buffer[str_len:]
        case ObjType.ARR:
            if len(buffer) < 4:
                raise NotEnoughData

            arr_len = int.from_bytes(buffer[:4], "little")
            buffer = buffer[4:]
            arr: list[RespObject] = []
            for _ in range(arr_len):
                elem, buffer = try_parse_object(buffer)
                arr.append(elem)
            return arr, buffer
        case _:
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

    def recv_resp(self) -> RespObject:
        """Receive a single response."""
        # TODO: Reduce the amount of copies
        while True:
            try:
                (resp_code, resp_obj), rest = try_parse_response(
                    memoryview(self.recv_buf)[: self.recv_len]
                )
                # Reset the buffers after a good parse
                self.recv_buf = bytearray(rest)
                self.recv_len = len(self.recv_buf)
                if resp_code == RespType.OK:
                    return resp_obj
                else:
                    if isinstance(resp_obj, bytes):
                        raise ResponseError(resp_obj)
                    else:
                        raise ProtocolError(
                            f"Unexpected error message type: {type(resp_obj)}"
                        )
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

    def send(self, request: ReqType, *args: ReqObject) -> RespObject:
        """Send and receive a single response."""
        self.send_req(request, *args)
        return self.recv_resp()

    def send_shutdown(self):
        """This needs some special handling since the server won't send a response."""
        try:
            val = self.send(ReqType.SHUTDOWN)
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

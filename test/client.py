import enum
import socket

class Request(enum.IntEnum):
    GET = 0
    SET = 1
    DEL = 2
    KEYS = 3


class Response(enum.IntEnum):
    OK = 0
    ERR = 1


class ObjType(enum.IntEnum):
    NIL = 0
    INT = 1
    STR = 2
    ARR = 3


class ProtocolError(Exception):
    pass
class ParseError(ProtocolError):
    pass
class UnexpectedEofError(ProtocolError):
    pass

class NotEnoughData(Exception):
    pass


def extend_with_arg(buffer, arg):
    if isinstance(arg, int):
        buffer.append(ObjType.INT)
        buffer.extend(arg.to_bytes(8, 'little'))
    elif isinstance(arg, str):
        buffer.append(ObjType.STR)
        # TODO: Encode as UTF-8?
        arg = arg.encode('ascii')
        buffer.extend(len(arg).to_bytes(4, 'little'))
        buffer.extend(arg)
    elif isinstance(arg, bytes):
        buffer.append(ObjType.STR)
        buffer.extend(len(arg).to_bytes(4, 'little'))
        buffer.extend(arg)
    else:
        raise TypeError(f'Invalid request argument type: {type(arg)}')


def try_parse_object(buffer):
    if len(buffer) == 0:
        raise NotEnoughData

    type_byte = buffer[0]
    buffer = buffer[1:]
    if type_byte == ObjType.NIL:
        return None, buffer
    elif type_byte == ObjType.INT:
        if len(buffer) < 8:
            raise NotEnoughData
        return int.from_bytes(buffer[:8], 'little'), buffer[8:]
    elif type_byte == ObjType.STR:
        if len(buffer) < 4:
            raise NotEnoughData
        str_len = int.from_bytes(buffer[:4], 'little')
        buffer = buffer[4:]
        if len(buffer) < str_len:
            raise NotEnoughData
        return bytes(buffer[:str_len]), buffer[str_len:]
    elif type_byte == ObjType.ARR:
        if len(buffer) < 4:
            raise NotEnoughData

        arr_len = int.from_bytes(buffer[:4], 'little')
        buffer = buffer[4:]
        arr = []
        for _ in range(arr_len):
            elem, buffer = try_parse_object(buffer)
            arr.append(elem)
        return arr, buffer
    else:
        raise ParseError(f'Invalid response type: f{type_byte}')


def try_parse_response(buffer):
    if len(buffer) == 0:
        raise NotEnoughData
    resp_code = Response(buffer[0])
    obj, rest = try_parse_object(buffer[1:])
    return (resp_code, obj), rest


class Client:
    def __init__(self, host='localhost', port=1234, *, timeout=None):
        self.conn = socket.create_connection((host, port), timeout=timeout)
        self.recv_buf = bytearray(4096)
        self.recv_len = 0

    def _send(self, request, *args):
        buffer = bytearray()
        buffer.append(request)
        for a in args:
            extend_with_arg(buffer, a)

        print('sending', buffer)
        self.conn.sendall(buffer)

    def _recv(self):
        # TODO: Reduce the amount of copies
        while True:
            try:
                resp, rest = try_parse_response(
                    memoryview(self.recv_buf)[:self.recv_len]
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

            chunk_len = self.conn.recv_into(memoryview(self.recv_buf)[self.recv_len:])
            print('received', self.recv_buf[self.recv_len:self.recv_len+chunk_len])
            if chunk_len == 0:
                raise UnexpectedEofError
            self.recv_len += chunk_len

    def send(self, request, *args):
        self._send(request, *args)
        return self._recv()

    def close(self):
        self.conn.close()

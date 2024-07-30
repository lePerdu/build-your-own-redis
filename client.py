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


def extend_with_arg(buffer, arg):
    if arg is None:
        buffer.append(ObjType.NIL)
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
    elif isinstance(arg, list):
        buffer.append(ObjType.ARR)
        buffer.extend(len(arg).to_bytes(4, 'little'))
        for a in arg:
            extend_with_arg(buffer, a)
    else:
        raise "invalid object type"


def parse_object(buffer):
    type_byte = buffer[0]
    buffer = buffer[1:]
    if type_byte == ObjType.NIL:
        return None, buffer
    elif type_byte == ObjType.INT:
        assert len(buffer) >= 8
        return int.from_bytes(buffer[:8], 'little'), buffer[8:]
    elif type_byte == ObjType.STR:
        assert len(buffer) >= 4
        str_len = int.from_bytes(buffer[:4], 'little')
        buffer = buffer[4:]
        assert len(buffer) >= str_len
        return buffer[:str_len], buffer[str_len:]
    elif type_byte == ObjType.ARR:
        assert len(buffer) >= 4
        arr_len = int.from_bytes(buffer[:4], 'little')
        buffer = buffer[4:]
        arr = []
        for _ in range(arr_len):
            elem, buffer = parse_object(buffer)
            arr.append(elem)
        return arr, buffer
    else:
        raise "invalid object type"


class Client:
    def __init__(self, host='localhost', port=1234):
        self.conn = socket.create_connection((host, port))

    def send(self, request, *args):
        buffer = bytearray()
        buffer.append(request)
        for a in args:
            extend_with_arg(buffer, a)

        full_msg = bytearray()
        full_msg.extend(len(buffer).to_bytes(4, 'little'))
        full_msg.extend(buffer)

        print('sending', full_msg)
        self.conn.sendall(full_msg)

        packet_len_buf = self.conn.recv(4, socket.MSG_WAITALL)
        assert len(packet_len_buf) == 4
        resp_len = int.from_bytes(packet_len_buf, 'little')
        resp_data = self.conn.recv(resp_len, socket.MSG_WAITALL)
        assert len(resp_data) == resp_len

        obj, rest = parse_object(resp_data[1:])
        assert len(rest) == 0
        return Response(resp_data[0]), obj

    def close(self):
        self.conn.close()

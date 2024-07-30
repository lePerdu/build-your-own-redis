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


def parse_object(buffer):
    type_byte = buffer[0]
    buffer = buffer[1:]
    if type_byte == ObjType.NIL:
        return None, buffer
    elif type_byte == ObjType.INT:
        if len(buffer) < 8:
            raise ParseError('not enough data')
        return int.from_bytes(buffer[:8], 'little'), buffer[8:]
    elif type_byte == ObjType.STR:
        if len(buffer) < 4:
            raise ParseError('not enough data')
        str_len = int.from_bytes(buffer[:4], 'little')
        buffer = buffer[4:]
        if len(buffer) < str_len:
            raise ParseError('not enough data')
        return buffer[:str_len], buffer[str_len:]
    elif type_byte == ObjType.ARR:
        if len(buffer) < 4:
            raise ParseError('not enough data')

        arr_len = int.from_bytes(buffer[:4], 'little')
        buffer = buffer[4:]
        arr = []
        for _ in range(arr_len):
            elem, buffer = parse_object(buffer)
            arr.append(elem)
        return arr, buffer
    else:
        raise ParseError(f'Invalid response type: f{type_byte}')


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
        if len(packet_len_buf) == 0:
            raise UnexpectedEofError
        elif len(packet_len_buf) != 4:
            raise UnexpectedEofError('Received partial message header')

        resp_len = int.from_bytes(packet_len_buf, 'little')
        resp_data = self.conn.recv(resp_len, socket.MSG_WAITALL)
        if len(resp_data) != resp_len:
            raise UnexpectedEofError('Received partial message data')
        print('recevied', packet_len_buf + resp_data)

        obj, rest = parse_object(resp_data[1:])
        if len(rest) > 0:
            raise ProtocolError('Message contains extra data')

        return Response(resp_data[0]), obj

    def close(self):
        self.conn.close()

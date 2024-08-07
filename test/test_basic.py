from client import ReqType, RespType
from test_util import Server, test


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

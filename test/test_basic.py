from client import Client, ReqType
from test_util import client_test


@client_test
def test_keys_empty_on_startup(c: Client):
    val = c.send(ReqType.KEYS)
    assert val == []


@client_test
def test_get_not_exist_returns_error(c: Client):
    val = c.send(ReqType.GET, "abc")
    assert val is None


@client_test
def test_get_returns_set_int_value(c: Client):
    val = c.send(ReqType.SET, "abc", 42)
    val = c.send(ReqType.GET, "abc")
    assert val == 42


@client_test
def test_get_returns_last_set_value(c: Client):
    val = c.send(ReqType.SET, "abc", 42)
    val = c.send(ReqType.SET, "abc", "def")
    val = c.send(ReqType.GET, "abc")
    assert val == b"def"


@client_test
def test_del_returns_false_if_not_present(c: Client):
    val = c.send(ReqType.DEL, "abc")
    assert val is False


@client_test
def test_del_returns_true_if_present(c: Client):
    _ = c.send(ReqType.SET, "abc", "def")
    val = c.send(ReqType.DEL, "abc")
    assert val is True


@client_test
def test_get_after_del_returns_err(c: Client):
    _ = c.send(ReqType.SET, "abc", "def")
    _ = c.send(ReqType.DEL, "abc")
    val = c.send(ReqType.GET, "abc")
    assert val is None


@client_test
def test_get_after_set_after_del_returns_value(c: Client):
    val = c.send(ReqType.SET, "abc", "def")
    val = c.send(ReqType.DEL, "abc")
    val = c.send(ReqType.SET, "abc", "new value")
    val = c.send(ReqType.GET, "abc")
    assert val == b"new value"


@client_test
def test_pipeline_set_get_commands(c: Client):
    c.send_req(ReqType.SET, "abc", "value")
    c.send_req(ReqType.GET, "abc")

    val = c.recv_resp()
    val = c.recv_resp()
    assert val == b"value"


@client_test
def test_set_and_get_10_000_keys(c: Client):
    n = 10_000

    # Pipeline to speed things up
    for i in range(n):
        c.send_req(ReqType.SET, f"key:{i}", f"value:{i}")
    for i in range(n):
        val = c.recv_resp()

    for i in range(n):
        c.send_req(ReqType.GET, f"key:{i}")
    for i in range(n):
        val = c.recv_resp()
        assert val == f"value:{i}".encode("ascii")

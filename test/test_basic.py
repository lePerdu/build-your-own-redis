import time

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
def test_get_returns_set_negative_int_value(c: Client):
    val = c.send(ReqType.SET, "abc", -42)
    val = c.send(ReqType.GET, "abc")
    assert val == -42


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
def test_set_get_del_10_000_keys(c: Client):
    n = 10_000

    # Pipeline to speed things up
    for i in range(n):
        c.send_req(ReqType.SET, f"key:{i}", f"value:{i}")
    for i in range(n):
        _ = c.recv_resp()

    for i in range(n):
        c.send_req(ReqType.GET, f"key:{i}")
    for i in range(n):
        val = c.recv_resp()
        assert val == f"value:{i}".encode("ascii")

    for i in range(n):
        c.send_req(ReqType.DEL, f"key:{i}")
    for i in range(n):
        val = c.recv_resp()
        assert val is True


@client_test
def test_ttl_is_minus_2_if_key_unset(c: Client):
    val = c.send(ReqType.TTL, "unset")
    print(f"{val=}")
    assert val == -2


@client_test
def test_ttl_is_minus_1_if_ttl_unset(c: Client):
    _ = c.send(ReqType.SET, "no-ttl", "forever")
    val = c.send(ReqType.TTL, "no-ttl")
    assert val == -1


TTL_EPSILON = 50


@client_test
def test_ttl_is_saved_after_expire(c: Client):
    _ = c.send(ReqType.SET, "expires", "temporary")
    _ = c.send(ReqType.EXPIRE, "expires", 5000)
    val = c.send(ReqType.TTL, "expires")
    assert isinstance(val, int)
    assert 5000 - TTL_EPSILON <= val <= 5000


@client_test
def test_key_expires_immediately_if_ttl_0(c: Client):
    _ = c.send(ReqType.SET, "expired", "temporary")
    _ = c.send(ReqType.EXPIRE, "expired", 0)
    val = c.send(ReqType.GET, "expired")
    assert val is None


@client_test
def test_key_expires_immediately_if_ttl_negative(c: Client):
    _ = c.send(ReqType.SET, "expired", "temporary")
    _ = c.send(ReqType.EXPIRE, "expired", -10)
    val = c.send(ReqType.GET, "expired")
    assert val is None


@client_test
def test_key_expires_soon_after_expiration_time(c: Client):
    _ = c.send(ReqType.SET, "expired", "temporary")
    _ = c.send(ReqType.EXPIRE, "expired", 500)
    time.sleep(0.5 + TTL_EPSILON / 1000.0)
    val = c.send(ReqType.GET, "expired")
    assert val is None


@client_test
def test_key_does_not_expire_if_ttl_changed_before_old_ttl(c: Client):
    _ = c.send(ReqType.SET, "not-expired", "still-there")
    _ = c.send(ReqType.EXPIRE, "not-expired", 500)
    _ = c.send(ReqType.EXPIRE, "not-expired", 10_000)
    time.sleep(0.5 + TTL_EPSILON / 1000.0)
    val = c.send(ReqType.GET, "not-expired")
    assert val == b"still-there"


@client_test
def test_key_does_not_expire_if_persisted_before_old_ttl(c: Client):
    _ = c.send(ReqType.SET, "not-expired", "still-there")
    _ = c.send(ReqType.EXPIRE, "not-expired", 500)
    _ = c.send(ReqType.PERSIST, "not-expired")
    time.sleep(0.5 + TTL_EPSILON / 1000.0)
    val = c.send(ReqType.GET, "not-expired")
    assert val == b"still-there"

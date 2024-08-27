import time

from client import Client
from test_util import client_test


@client_test
def test_keys_empty_on_startup(c: Client):
    val = c.send("KEYS")
    assert val == []


@client_test
def test_get_not_exist_returns_error(c: Client):
    val = c.send("GET", "abc")
    assert val is None


@client_test
def test_set_returns_ok_if_successful(c: Client):
    val = c.send("SET", "abc", 42)
    assert val == b"OK"
    # Also for overriding value
    val = c.send("SET", "abc", "def")
    assert val == b"OK"


@client_test
def test_get_returns_last_set_value(c: Client):
    _ = c.send("SET", "abc", 42)
    _ = c.send("SET", "abc", "def")
    val = c.send("GET", "abc")
    assert val == b"def"


@client_test
def test_del_returns_0_if_not_present(c: Client):
    val = c.send("DEL", "abc")
    assert val == 0


@client_test
def test_del_returns_1_if_present(c: Client):
    val = c.send("SET", "abc", "def")
    val = c.send("DEL", "abc")
    assert val == 1


@client_test
def test_get_after_del_returns_err(c: Client):
    _ = c.send("SET", "abc", "def")
    _ = c.send("DEL", "abc")
    val = c.send("GET", "abc")
    assert val is None


@client_test
def test_get_after_set_after_del_returns_value(c: Client):
    val = c.send("SET", "abc", "def")
    val = c.send("DEL", "abc")
    val = c.send("SET", "abc", "new value")
    val = c.send("GET", "abc")
    assert val == b"new value"


@client_test
def test_pipeline_set_get_commands(c: Client):
    c.send_req("SET", "abc", "value")
    c.send_req("GET", "abc")

    val = c.recv_resp()
    val = c.recv_resp()
    assert val == b"value"


@client_test
def test_set_get_del_10_000_keys(c: Client):
    n = 10_000

    # Pipeline to speed things up
    for i in range(n):
        c.send_req("SET", f"key:{i}", f"value:{i}")
    for i in range(n):
        _ = c.recv_resp()

    for i in range(n):
        c.send_req("GET", f"key:{i}")
    for i in range(n):
        val = c.recv_resp()
        assert val == f"value:{i}".encode("ascii")

    for i in range(n):
        c.send_req("DEL", f"key:{i}")
    for i in range(n):
        val = c.recv_resp()
        assert val == 1


@client_test
def test_ttl_is_minus_2_if_key_unset(c: Client):
    val = c.send("TTL", "unset")
    print(f"{val=}")
    assert val == -2


@client_test
def test_ttl_is_minus_1_if_ttl_unset(c: Client):
    _ = c.send("SET", "no-ttl", "forever")
    val = c.send("TTL", "no-ttl")
    assert val == -1


TTL_EPSILON = 50


@client_test
def test_ttl_is_saved_after_expire(c: Client):
    _ = c.send("SET", "expires", "temporary")
    _ = c.send("EXPIRE", "expires", 5000)
    val = c.send("TTL", "expires")
    assert isinstance(val, int)
    assert 5000 - TTL_EPSILON <= val <= 5000


@client_test
def test_key_expires_immediately_if_ttl_0(c: Client):
    _ = c.send("SET", "expired", "temporary")
    _ = c.send("EXPIRE", "expired", 0)
    val = c.send("GET", "expired")
    assert val is None


@client_test
def test_key_expires_immediately_if_ttl_negative(c: Client):
    _ = c.send("SET", "expired", "temporary")
    _ = c.send("EXPIRE", "expired", -10)
    val = c.send("GET", "expired")
    assert val is None


@client_test
def test_key_expires_soon_after_expiration_time(c: Client):
    _ = c.send("SET", "expired", "temporary")
    _ = c.send("EXPIRE", "expired", 500)
    time.sleep(0.5 + TTL_EPSILON / 1000.0)
    val = c.send("GET", "expired")
    assert val is None


@client_test
def test_key_does_not_expire_if_ttl_changed_before_old_ttl(c: Client):
    _ = c.send("SET", "not-expired", "still-there")
    _ = c.send("EXPIRE", "not-expired", 500)
    _ = c.send("EXPIRE", "not-expired", 10_000)
    time.sleep(0.5 + TTL_EPSILON / 1000.0)
    val = c.send("GET", "not-expired")
    assert val == b"still-there"


@client_test
def test_key_does_not_expire_if_persisted_before_old_ttl(c: Client):
    _ = c.send("SET", "not-expired", "still-there")
    _ = c.send("EXPIRE", "not-expired", 500)
    _ = c.send("PERSIST", "not-expired")
    time.sleep(0.5 + TTL_EPSILON / 1000.0)
    val = c.send("GET", "not-expired")
    assert val == b"still-there"

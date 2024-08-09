from client import Client, ReqType, ResponseError, resp_object_dict
from test_util import client_test


@client_test
def test_zscore_returns_value_after_zadd(c: Client):
    _ = c.send(ReqType.ZADD, "scores", 4.32, "field")
    val = c.send(ReqType.ZSCORE, "scores", "field")
    assert val == 4.32


@client_test
def test_zscore_returns_value_after_latest_zadd(c: Client):
    _ = c.send(ReqType.ZADD, "scores", 4.32, "field")
    _ = c.send(ReqType.ZADD, "scores", 5.9, "field")

    val = c.send(ReqType.ZSCORE, "scores", "field")
    assert val == 5.9


@client_test
def test_zscore_missing_key(c: Client):
    val = c.send(ReqType.ZSCORE, "scores", "field")
    assert val is None


@client_test
def test_zscore_missing_field(c: Client):
    _ = c.send(ReqType.ZADD, "scores", 16.0, "field1")
    val = c.send(ReqType.ZSCORE, "scores", "field2")
    assert val is None


@client_test
def test_zcard_0_if_not_found(c: Client):
    val = c.send(ReqType.ZCARD, "scores")
    assert val == 0


@client_test
def test_zcard_err_if_not_hash(c: Client):
    _ = c.send(ReqType.SET, "scores", "projection")
    try:
        _ = c.send(ReqType.ZCARD, "scores")
    except ResponseError:
        return
    assert False, "Expected ResponseError"


@client_test
def test_zcard_counts_unique_set_keys(c: Client):
    _ = c.send(ReqType.ZADD, "scores", 9.2, "pigs")
    _ = c.send(ReqType.ZADD, "scores", 2.3, "cows")
    _ = c.send(ReqType.ZADD, "scores", 1.6, "farmers")
    # Update shouldn't increase len
    _ = c.send(ReqType.ZADD, "scores", 0.1, "pigs")

    val = c.send(ReqType.ZCARD, "scores")
    assert val == 3


@client_test
def test_zcard_decreases_after_zrem(c: Client):
    _ = c.send(ReqType.ZADD, "scores", 9.2, "pigs")
    _ = c.send(ReqType.ZADD, "scores", 2.3, "cows")
    _ = c.send(ReqType.ZADD, "scores", 1.6, "farmers")
    # Update shouldn't increase len
    _ = c.send(ReqType.ZREM, "scores", "pigs")

    val = c.send(ReqType.ZCARD, "scores")
    assert val == 2


@client_test
def test_zadd_and_zscore_10_000_keys(c: Client):
    n = 10_000

    for i in range(n):
        # Store the hash so that keys aren't inserted in order
        c.send_req(ReqType.ZADD, "scores", float(hash(i)), f"key:{i}")
    for i in range(n):
        val = c.recv_resp()

    for i in range(n):
        c.send_req(ReqType.ZSCORE, "scores", f"key:{i}")
    for i in range(n):
        val = c.recv_resp()
        assert val == float(hash(i))

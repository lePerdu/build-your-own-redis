from client import Client, ResponseError, resp_object_dict
from test_util import client_test


@client_test
def test_hget_returns_value_after_hset(c: Client):
    _ = c.send("HSET", "map", "field", "value")
    val = c.send("HGET", "map", "field")
    assert val == b"value"


@client_test
def test_hget_returns_value_after_latest_hset(c: Client):
    _ = c.send("HSET", "map", "field", "old-val")
    _ = c.send("HSET", "map", "field", "new-val")

    val = c.send("HGET", "map", "field")
    assert val == b"new-val"


@client_test
def test_type_is_hash_after_hset(c: Client):
    _ = c.send("HSET", "map", "field", "val")
    val = c.send("TYPE", "map")
    assert val == b"hash"


@client_test
def test_hget_missing_key(c: Client):
    val = c.send("HGET", "map", "field")
    assert val is None


@client_test
def test_hget_missing_field(c: Client):
    _ = c.send("HSET", "map", "field1", "value")
    val = c.send("HGET", "map", "field2")
    assert val is None


@client_test
def test_hlen_0_if_not_found(c: Client):
    val = c.send("HLEN", "map")
    assert val == 0


@client_test
def test_hlen_err_if_not_hash(c: Client):
    _ = c.send("SET", "map", "projection")
    try:
        _ = c.send("HLEN", "map")
    except ResponseError:
        return
    assert False, "Expected ResponseError"


@client_test
def test_hlen_counts_unique_set_keys(c: Client):
    _ = c.send("HSET", "map", "pigs", 1)
    _ = c.send("HSET", "map", "cows", "nope")
    _ = c.send("HSET", "map", "farmers", 2)
    # Update shouldn't increase len
    _ = c.send("HSET", "map", "pigs", 3)

    val = c.send("HLEN", "map")
    assert val == 3


@client_test
def test_hlen_decreases_after_hdel(c: Client):
    _ = c.send("HSET", "map", "pigs", 1)
    _ = c.send("HSET", "map", "cows", "nope")
    _ = c.send("HSET", "map", "farmers", 2)
    # Update shouldn't increase len
    _ = c.send("HDEL", "map", "pigs")

    val = c.send("HLEN", "map")
    assert val == 2


@client_test
def test_hkeys_empty_if_not_found(c: Client):
    val = c.send("HKEYS", "map")
    assert val == []


@client_test
def test_hkeys_err_if_not_hash(c: Client):
    _ = c.send("SET", "map", "projection")
    try:
        _ = c.send("HKEYS", "map")
    except ResponseError:
        return
    assert False, "Expected ResponseError"


@client_test
def test_hkeys_counts_unique_set_keys(c: Client):
    _ = c.send("HSET", "map", "pigs", 1)
    _ = c.send("HSET", "map", "cows", "nope")
    _ = c.send("HSET", "map", "farmers", 2)
    # Update shouldn't increase len
    _ = c.send("HSET", "map", "pigs", 3)

    val = c.send("HKEYS", "map")
    assert isinstance(val, list)
    assert set(val) == {b"pigs", b"cows", b"farmers"}


@client_test
def test_hkeys_decreases_after_hdel(c: Client):
    _ = c.send("HSET", "map", "pigs", 1)
    _ = c.send("HSET", "map", "cows", "nope")
    _ = c.send("HSET", "map", "farmers", 2)
    # Update shouldn't increase len
    _ = c.send("HDEL", "map", "pigs")

    val = c.send("HKEYS", "map")
    assert isinstance(val, list)
    assert set(val) == {b"cows", b"farmers"}


@client_test
def test_hgetall_0_if_not_found(c: Client):
    val = c.send("HGETALL", "map")
    assert val == []


@client_test
def test_hgetall_err_if_not_hash(c: Client):
    _ = c.send("SET", "map", "projection")
    try:
        _ = c.send("HGETALL", "map")
    except ResponseError:
        return
    assert False, "Expected ResponseError"


@client_test
def test_hgetall_has_unique_set_keys(c: Client):
    _ = c.send("HSET", "map", "pigs", "one")
    _ = c.send("HSET", "map", "cows", "nope")
    _ = c.send("HSET", "map", "farmers", "2")
    # Update shouldn't increase len
    _ = c.send("HSET", "map", "pigs", "three")

    val = c.send("HGETALL", "map")
    assert isinstance(val, list)
    assert len(val) == 6
    assert resp_object_dict(val) == {
        b"pigs": b"three",
        b"cows": b"nope",
        b"farmers": b"2",
    }


@client_test
def test_hgetall_decreases_after_hdel(c: Client):
    _ = c.send("HSET", "map", "pigs", "one")
    _ = c.send("HSET", "map", "cows", "nope")
    _ = c.send("HSET", "map", "farmers", "2")
    # Update shouldn't increase len
    _ = c.send("HDEL", "map", "pigs")

    val = c.send("HGETALL", "map")
    assert isinstance(val, list)
    assert len(val) == 4
    assert resp_object_dict(val) == {b"cows": b"nope", b"farmers": b"2"}


@client_test
def test_hset_hget_del_10_000_keys(c: Client):
    n = 10_000

    for i in range(n):
        c.send_req("HSET", "hash", f"key:{i}", f"value:{i}")
    for i in range(n):
        val = c.recv_resp()

    for i in range(n):
        c.send_req("HGET", "hash", f"key:{i}")
    for i in range(n):
        val = c.recv_resp()
        assert val == f"value:{i}".encode("ascii")

    val = c.send("DEL", "hash")
    assert val == 1


@client_test
def test_get_returns_nil_after_deleting_hash(c: Client):
    _ = c.send("HSET", "map", "field", "value")

    _ = c.send("DEL", "map")

    val = c.send("GET", "map")
    assert val is None

from client import Client, ResponseError
from test_util import client_test


@client_test
def test_sadd_returns_1_if_missign(c: Client):
    val = c.send("SADD", "set", "key")
    assert val == 1


@client_test
def test_sadd_returns_0_if_already_added(c: Client):
    val = c.send("SADD", "set", "key")
    val = c.send("SADD", "set", "key")
    assert val == 0


@client_test
def test_srem_returns_1_if_exists(c: Client):
    _ = c.send("SADD", "set", "key")
    val = c.send("SREM", "set", "key")
    assert val == 1


@client_test
def test_srem_returns_0_if_missing(c: Client):
    _ = c.send("SADD", "set", "key1")
    val = c.send("SREM", "set", "key2")
    assert val == 0


@client_test
def test_srem_returns_0_if_set_not_created(c: Client):
    val = c.send("SREM", "set", "key2")
    assert val == 0


@client_test
def test_sadd_returns_1_if_readded(c: Client):
    val = c.send("SADD", "set", "key")
    val = c.send("SREM", "set", "key")
    val = c.send("SADD", "set", "key")
    assert val == 1


@client_test
def test_sismember_returns_1_after_sadd(c: Client):
    _ = c.send("SADD", "set", "key")
    val = c.send("SISMEMBER", "set", "key")
    assert val == 1


@client_test
def test_sismember_returns_1_after_multiple_sadds(c: Client):
    _ = c.send("SADD", "set", "key1")
    _ = c.send("SADD", "set", "key1")

    val = c.send("SISMEMBER", "set", "key1")
    assert val == 1


@client_test
def test_sismember_returns_0_after_srem(c: Client):
    _ = c.send("SADD", "set", "key")
    _ = c.send("SREM", "set", "key")
    val = c.send("SISMEMBER", "set", "key")
    assert val == 0


@client_test
def test_sismember_returns_0_if_set_not_created(c: Client):
    val = c.send("SISMEMBER", "set", "key")
    assert val == 0


@client_test
def test_sismember_returns_0_if_key_missign(c: Client):
    _ = c.send("SADD", "set", "key1")
    val = c.send("SISMEMBER", "set", "key2")
    assert val == 0


@client_test
def test_scard_0_if_not_found(c: Client):
    val = c.send("SCARD", "set")
    assert val == 0


@client_test
def test_scard_err_if_not_hash(c: Client):
    _ = c.send("SET", "set", "projection")
    try:
        _ = c.send("SCARD", "set")
    except ResponseError:
        return
    assert False, "Expected ResponseError"


@client_test
def test_scard_counts_unique_set_keys(c: Client):
    _ = c.send("SADD", "set", "pigs")
    _ = c.send("SADD", "set", "cows")
    _ = c.send("SADD", "set", "farmers")
    # Update shouldn't increase len
    _ = c.send("SADD", "set", "pigs")

    val = c.send("SCARD", "set")
    assert val == 3


@client_test
def test_scard_decreases_after_hdel(c: Client):
    _ = c.send("SADD", "set", "pigs")
    _ = c.send("SADD", "set", "cows")
    _ = c.send("SADD", "set", "farmers")
    # Update shouldn't increase len
    _ = c.send("SREM", "set", "pigs")

    val = c.send("SCARD", "set")
    assert val == 2


@client_test
def test_smembers_empty_if_not_found(c: Client):
    val = c.send("SMEMBERS", "set")
    assert val == []


@client_test
def test_smembers_err_if_not_hash(c: Client):
    _ = c.send("SET", "set", "projection")
    try:
        _ = c.send("SMEMBERS", "set")
    except ResponseError:
        return
    assert False, "Expected ResponseError"


@client_test
def test_smembers_counts_unique_set_keys(c: Client):
    _ = c.send("SADD", "set", "pigs")
    _ = c.send("SADD", "set", "cows")
    _ = c.send("SADD", "set", "farmers")
    # Update shouldn't increase len
    _ = c.send("SADD", "set", "pigs")

    val = c.send("SMEMBERS", "set")
    assert isinstance(val, list)
    assert set(val) == {b"pigs", b"cows", b"farmers"}


@client_test
def test_smembers_decreases_after_hdel(c: Client):
    _ = c.send("SADD", "set", "pigs")
    _ = c.send("SADD", "set", "cows")
    _ = c.send("SADD", "set", "farmers")
    # Update shouldn't increase len
    _ = c.send("SREM", "set", "pigs")

    val = c.send("SMEMBERS", "set")
    assert isinstance(val, list)
    assert set(val) == {b"cows", b"farmers"}


@client_test
def test_sadd_sismember_del_10_000_keys(c: Client):
    n = 10_000

    for i in range(n):
        c.send_req("SADD", "hash", f"key:{i}")
    for i in range(n):
        val = c.recv_resp()

    for i in range(n):
        c.send_req("SISMEMBER", "hash", f"key:{i}")
    for i in range(n):
        val = c.recv_resp()
        assert val == 1

    val = c.send("DEL", "hash")
    assert val == 1


@client_test
def test_overwrite_set_with_scalar(c: Client):
    _ = c.send("SADD", "set", "key1")

    _ = c.send("SET", "set", "scalar-value")

    val = c.send("GET", "set")
    assert val == b"scalar-value"


@client_test
def test_get_returns_nil_after_deleting_set(c: Client):
    _ = c.send("SADD", "set", "key1")

    _ = c.send("DEL", "set")

    val = c.send("GET", "set")
    assert val is None


@client_test
def test_overwrite_hash_with_scalar(c: Client):
    _ = c.send("HSET", "set", "key", "value")

    _ = c.send("SET", "set", "scalar-value")

    val = c.send("GET", "set")
    assert val == b"scalar-value"


@client_test
def test_srandmember_nil_if_set_not_created(c: Client):
    val = c.send("SRANDMEMBER", "set")
    assert val is None


@client_test
def test_srandmember_nil_if_set_empty(c: Client):
    val = c.send("SADD", "set", "key1")
    val = c.send("SREM", "set", "key1")
    val = c.send("SRANDMEMBER", "set")
    assert val is None


@client_test
def test_srandmember_returns_element_if_nonempty(c: Client):
    val = c.send("SADD", "set", "key1")
    val = c.send("SADD", "set", "key2")
    val = c.send("SRANDMEMBER", "set")
    assert val == b"key1" or val == b"key2"


@client_test
def test_spop_nil_if_set_not_created(c: Client):
    val = c.send("SPOP", "set")
    assert val is None


@client_test
def test_spop_nil_if_set_empty(c: Client):
    val = c.send("SADD", "set", "key1")
    val = c.send("SREM", "set", "key1")
    val = c.send("SPOP", "set")
    assert val is None


@client_test
def test_sismember_returns_0_after_spop(c: Client):
    val = c.send("SADD", "set", "key1")
    val = c.send("SPOP", "set")
    val = c.send("SISMEMBER", "set", "key1")
    assert val == 0


@client_test
def test_spop_returns_all_elements_when_called_repeatedly(c: Client):
    _ = c.send("SADD", "set", "key1")
    _ = c.send("SADD", "set", "key2")
    val1 = c.send("SPOP", "set")
    assert isinstance(val1, bytes)
    val2 = c.send("SPOP", "set")
    assert isinstance(val2, bytes)

    assert {val1, val2} == {b"key1", b"key2"}

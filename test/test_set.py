from client import Client, ReqType, ResponseError
from test_util import client_test


@client_test
def test_sadd_returns_true_if_missign(c: Client):
    val = c.send(ReqType.SADD, "set", "key")
    assert val is True


@client_test
def test_sadd_returns_true_if_already_added(c: Client):
    val = c.send(ReqType.SADD, "set", "key")
    val = c.send(ReqType.SADD, "set", "key")
    assert val is False


@client_test
def test_srem_returns_true_if_exists(c: Client):
    _ = c.send(ReqType.SADD, "set", "key")
    val = c.send(ReqType.SREM, "set", "key")
    assert val is True


@client_test
def test_srem_returns_false_if_missing(c: Client):
    _ = c.send(ReqType.SADD, "set", "key1")
    val = c.send(ReqType.SREM, "set", "key2")
    assert val is False


@client_test
def test_srem_returns_false_if_set_not_created(c: Client):
    val = c.send(ReqType.SREM, "set", "key2")
    assert val is False


@client_test
def test_sadd_returns_true_if_readded(c: Client):
    val = c.send(ReqType.SADD, "set", "key")
    val = c.send(ReqType.SREM, "set", "key")
    val = c.send(ReqType.SADD, "set", "key")
    assert val is True


@client_test
def test_sismember_returns_true_after_sadd(c: Client):
    _ = c.send(ReqType.SADD, "set", "key")
    val = c.send(ReqType.SISMEMBER, "set", "key")
    assert val is True


@client_test
def test_sismember_returns_true_after_multiple_sadds(c: Client):
    _ = c.send(ReqType.SADD, "set", "key1")
    _ = c.send(ReqType.SADD, "set", "key1")

    val = c.send(ReqType.SISMEMBER, "set", "key1")
    assert val is True


@client_test
def test_sismember_returns_false_after_srem(c: Client):
    _ = c.send(ReqType.SADD, "set", "key")
    _ = c.send(ReqType.SREM, "set", "key")
    val = c.send(ReqType.SISMEMBER, "set", "key")
    assert val is False


@client_test
def test_sismember_returns_false_if_set_not_created(c: Client):
    val = c.send(ReqType.SISMEMBER, "set", "key")
    assert val is False


@client_test
def test_sismember_returns_false_if_key_missign(c: Client):
    _ = c.send(ReqType.SADD, "set", "key1")
    val = c.send(ReqType.SISMEMBER, "set", "key2")
    assert val is False


@client_test
def test_scard_0_if_not_found(c: Client):
    val = c.send(ReqType.SCARD, "set")
    assert val == 0


@client_test
def test_scard_err_if_not_hash(c: Client):
    _ = c.send(ReqType.SET, "set", "projection")
    try:
        _ = c.send(ReqType.SCARD, "set")
    except ResponseError:
        return
    assert False, "Expected ResponseError"


@client_test
def test_scard_counts_unique_set_keys(c: Client):
    _ = c.send(ReqType.SADD, "set", "pigs")
    _ = c.send(ReqType.SADD, "set", "cows")
    _ = c.send(ReqType.SADD, "set", "farmers")
    # Update shouldn't increase len
    _ = c.send(ReqType.SADD, "set", "pigs")

    val = c.send(ReqType.SCARD, "set")
    assert val == 3


@client_test
def test_scard_decreases_after_hdel(c: Client):
    _ = c.send(ReqType.SADD, "set", "pigs")
    _ = c.send(ReqType.SADD, "set", "cows")
    _ = c.send(ReqType.SADD, "set", "farmers")
    # Update shouldn't increase len
    _ = c.send(ReqType.SREM, "set", "pigs")

    val = c.send(ReqType.SCARD, "set")
    assert val == 2


@client_test
def test_smembers_empty_if_not_found(c: Client):
    val = c.send(ReqType.SMEMBERS, "set")
    assert val == []


@client_test
def test_smembers_err_if_not_hash(c: Client):
    _ = c.send(ReqType.SET, "set", "projection")
    try:
        _ = c.send(ReqType.SMEMBERS, "set")
    except ResponseError:
        return
    assert False, "Expected ResponseError"


@client_test
def test_smembers_counts_unique_set_keys(c: Client):
    _ = c.send(ReqType.SADD, "set", "pigs")
    _ = c.send(ReqType.SADD, "set", "cows")
    _ = c.send(ReqType.SADD, "set", "farmers")
    # Update shouldn't increase len
    _ = c.send(ReqType.SADD, "set", "pigs")

    val = c.send(ReqType.SMEMBERS, "set")
    assert isinstance(val, list)
    assert set(val) == {b"pigs", b"cows", b"farmers"}


@client_test
def test_smembers_decreases_after_hdel(c: Client):
    _ = c.send(ReqType.SADD, "set", "pigs")
    _ = c.send(ReqType.SADD, "set", "cows")
    _ = c.send(ReqType.SADD, "set", "farmers")
    # Update shouldn't increase len
    _ = c.send(ReqType.SREM, "set", "pigs")

    val = c.send(ReqType.SMEMBERS, "set")
    assert isinstance(val, list)
    assert set(val) == {b"cows", b"farmers"}


@client_test
def test_sadd_sismember_del_10_000_keys(c: Client):
    n = 10_000

    for i in range(n):
        c.send_req(ReqType.SADD, "hash", f"key:{i}")
    for i in range(n):
        val = c.recv_resp()

    for i in range(n):
        c.send_req(ReqType.SISMEMBER, "hash", f"key:{i}")
    for i in range(n):
        val = c.recv_resp()
        assert val is True

    val = c.send(ReqType.DEL, "hash")
    assert val is True


@client_test
def test_overwrite_set_with_scalar(c: Client):
    _ = c.send(ReqType.SADD, "set", "key1")

    _ = c.send(ReqType.SET, "set", "scalar-value")

    val = c.send(ReqType.GET, "set")
    assert val == b"scalar-value"


@client_test
def test_get_returns_nil_after_deleting_set(c: Client):
    _ = c.send(ReqType.SADD, "set", "key1")

    _ = c.send(ReqType.DEL, "set")

    val = c.send(ReqType.GET, "set")
    assert val is None


@client_test
def test_overwrite_hash_with_scalar(c: Client):
    _ = c.send(ReqType.HSET, "set", "key", "value")

    _ = c.send(ReqType.SET, "set", "scalar-value")

    val = c.send(ReqType.GET, "set")
    assert val == b"scalar-value"


@client_test
def test_srandmember_nil_if_set_not_created(c: Client):
    val = c.send(ReqType.SRANDMEMBER, "set")
    assert val is None


@client_test
def test_srandmember_nil_if_set_empty(c: Client):
    val = c.send(ReqType.SADD, "set", "key1")
    val = c.send(ReqType.SREM, "set", "key1")
    val = c.send(ReqType.SRANDMEMBER, "set")
    assert val is None


@client_test
def test_srandmember_returns_element_if_nonempty(c: Client):
    val = c.send(ReqType.SADD, "set", "key1")
    val = c.send(ReqType.SADD, "set", "key2")
    val = c.send(ReqType.SRANDMEMBER, "set")
    assert val == b"key1" or val == b"key2"


@client_test
def test_spop_nil_if_set_not_created(c: Client):
    val = c.send(ReqType.SPOP, "set")
    assert val is None


@client_test
def test_spop_nil_if_set_empty(c: Client):
    val = c.send(ReqType.SADD, "set", "key1")
    val = c.send(ReqType.SREM, "set", "key1")
    val = c.send(ReqType.SPOP, "set")
    assert val is None


@client_test
def test_sismember_returns_false_after_spop(c: Client):
    val = c.send(ReqType.SADD, "set", "key1")
    val = c.send(ReqType.SPOP, "set")
    val = c.send(ReqType.SISMEMBER, "set", "key1")
    assert val is False


@client_test
def test_spop_returns_all_elements_when_called_repeatedly(c: Client):
    _ = c.send(ReqType.SADD, "set", "key1")
    _ = c.send(ReqType.SADD, "set", "key2")
    val1 = c.send(ReqType.SPOP, "set")
    assert isinstance(val1, bytes)
    val2 = c.send(ReqType.SPOP, "set")
    assert isinstance(val2, bytes)

    assert {val1, val2} == {b"key1", b"key2"}

from client import ReqType, ResponseError
from test_util import Server, test


@test
def test_sadd_returns_true_if_missign(server: Server):
    c = server.make_client()
    val = c.send(ReqType.SADD, "set", "key")
    assert val is True


@test
def test_sadd_returns_true_if_already_added(server: Server):
    c = server.make_client()
    val = c.send(ReqType.SADD, "set", "key")
    val = c.send(ReqType.SADD, "set", "key")
    assert val is False


@test
def test_srem_returns_true_if_exists(server: Server):
    c = server.make_client()
    _ = c.send(ReqType.SADD, "set", "key")
    val = c.send(ReqType.SREM, "set", "key")
    assert val is True


@test
def test_srem_returns_false_if_missing(server: Server):
    c = server.make_client()
    _ = c.send(ReqType.SADD, "set", "key1")
    val = c.send(ReqType.SREM, "set", "key2")
    assert val is False


@test
def test_srem_returns_false_if_set_not_created(server: Server):
    c = server.make_client()
    val = c.send(ReqType.SREM, "set", "key2")
    assert val is False


@test
def test_sadd_returns_true_if_readded(server: Server):
    c = server.make_client()
    val = c.send(ReqType.SADD, "set", "key")
    val = c.send(ReqType.SREM, "set", "key")
    val = c.send(ReqType.SADD, "set", "key")
    assert val is True


@test
def test_sismember_returns_true_after_sadd(server: Server):
    c = server.make_client()
    _ = c.send(ReqType.SADD, "set", "key")
    val = c.send(ReqType.SISMEMBER, "set", "key")
    assert val is True


@test
def test_sismember_returns_true_after_multiple_sadds(server: Server):
    c = server.make_client()
    _ = c.send(ReqType.SADD, "set", "key1")
    _ = c.send(ReqType.SADD, "set", "key1")

    val = c.send(ReqType.SISMEMBER, "set", "key1")
    assert val is True


@test
def test_sismember_returns_false_after_srem(server: Server):
    c = server.make_client()
    _ = c.send(ReqType.SADD, "set", "key")
    _ = c.send(ReqType.SREM, "set", "key")
    val = c.send(ReqType.SISMEMBER, "set", "key")
    assert val is False


@test
def test_sismember_returns_false_if_set_not_created(server: Server):
    c = server.make_client()
    val = c.send(ReqType.SISMEMBER, "set", "key")
    assert val is False


@test
def test_sismember_returns_false_if_key_missign(server: Server):
    c = server.make_client()
    _ = c.send(ReqType.SADD, "set", "key1")
    val = c.send(ReqType.SISMEMBER, "set", "key2")
    assert val is False


@test
def test_scard_0_if_not_found(server: Server):
    c = server.make_client()
    val = c.send(ReqType.SCARD, "set")
    assert val == 0


@test
def test_scard_err_if_not_hash(server: Server):
    c = server.make_client()
    _ = c.send(ReqType.SET, "set", "projection")
    try:
        _ = c.send(ReqType.SCARD, "set")
    except ResponseError:
        return
    assert False, "Expected ResponseError"


@test
def test_scard_counts_unique_set_keys(server: Server):
    c = server.make_client()
    _ = c.send(ReqType.SADD, "set", "pigs")
    _ = c.send(ReqType.SADD, "set", "cows")
    _ = c.send(ReqType.SADD, "set", "farmers")
    # Update shouldn't increase len
    _ = c.send(ReqType.SADD, "set", "pigs")

    val = c.send(ReqType.SCARD, "set")
    assert val == 3


@test
def test_scard_decreases_after_hdel(server: Server):
    c = server.make_client()
    _ = c.send(ReqType.SADD, "set", "pigs")
    _ = c.send(ReqType.SADD, "set", "cows")
    _ = c.send(ReqType.SADD, "set", "farmers")
    # Update shouldn't increase len
    _ = c.send(ReqType.SREM, "set", "pigs")

    val = c.send(ReqType.SCARD, "set")
    assert val == 2


@test
def test_smembers_empty_if_not_found(server: Server):
    c = server.make_client()
    val = c.send(ReqType.SMEMBERS, "set")
    assert val == []


@test
def test_smembers_err_if_not_hash(server: Server):
    c = server.make_client()
    _ = c.send(ReqType.SET, "set", "projection")
    try:
        _ = c.send(ReqType.SMEMBERS, "set")
    except ResponseError:
        return
    assert False, "Expected ResponseError"


@test
def test_smembers_counts_unique_set_keys(server: Server):
    c = server.make_client()
    _ = c.send(ReqType.SADD, "set", "pigs")
    _ = c.send(ReqType.SADD, "set", "cows")
    _ = c.send(ReqType.SADD, "set", "farmers")
    # Update shouldn't increase len
    _ = c.send(ReqType.SADD, "set", "pigs")

    val = c.send(ReqType.SMEMBERS, "set")
    assert isinstance(val, list)
    assert set(val) == {b"pigs", b"cows", b"farmers"}


@test
def test_smembers_decreases_after_hdel(server: Server):
    c = server.make_client()
    _ = c.send(ReqType.SADD, "set", "pigs")
    _ = c.send(ReqType.SADD, "set", "cows")
    _ = c.send(ReqType.SADD, "set", "farmers")
    # Update shouldn't increase len
    _ = c.send(ReqType.SREM, "set", "pigs")

    val = c.send(ReqType.SMEMBERS, "set")
    assert isinstance(val, list)
    assert set(val) == {b"cows", b"farmers"}


@test
def test_sadd_and_sismember_10_000_keys(server: Server):
    n = 10_000
    c = server.make_client()

    for i in range(n):
        c.send_req(ReqType.SADD, "hash", f"key:{i}")
    for i in range(n):
        val = c.recv_resp()

    for i in range(n):
        c.send_req(ReqType.SISMEMBER, "hash", f"key:{i}")
    for i in range(n):
        val = c.recv_resp()
        assert val is True


@test
def test_overwrite_set_with_scalar(server: Server):
    c = server.make_client()
    _ = c.send(ReqType.SADD, "set", "key1")

    _ = c.send(ReqType.SET, "set", "scalar-value")

    val = c.send(ReqType.GET, "set")
    assert val == b"scalar-value"


@test
def test_get_returns_nil_after_deleting_set(server: Server):
    c = server.make_client()
    _ = c.send(ReqType.SADD, "set", "key1")

    _ = c.send(ReqType.DEL, "set")

    val = c.send(ReqType.GET, "set")
    assert val is None


@test
def test_overwrite_hash_with_scalar(server: Server):
    c = server.make_client()
    _ = c.send(ReqType.HSET, "set", "key", "value")

    _ = c.send(ReqType.SET, "set", "scalar-value")

    val = c.send(ReqType.GET, "set")
    assert val == b"scalar-value"


@test
def test_srandmember_nil_if_set_not_created(server: Server):
    c = server.make_client()
    val = c.send(ReqType.SRANDMEMBER, "set")
    assert val is None


@test
def test_srandmember_nil_if_set_empty(server: Server):
    c = server.make_client()
    val = c.send(ReqType.SADD, "set", "key1")
    val = c.send(ReqType.SREM, "set", "key1")
    val = c.send(ReqType.SRANDMEMBER, "set")
    assert val is None


@test
def test_srandmember_returns_element_if_nonempty(server: Server):
    c = server.make_client()
    val = c.send(ReqType.SADD, "set", "key1")
    val = c.send(ReqType.SADD, "set", "key2")
    val = c.send(ReqType.SRANDMEMBER, "set")
    assert val == b"key1" or val == b"key2"


@test
def test_spop_nil_if_set_not_created(server: Server):
    c = server.make_client()
    val = c.send(ReqType.SPOP, "set")
    assert val is None


@test
def test_spop_nil_if_set_empty(server: Server):
    c = server.make_client()
    val = c.send(ReqType.SADD, "set", "key1")
    val = c.send(ReqType.SREM, "set", "key1")
    val = c.send(ReqType.SPOP, "set")
    assert val is None


@test
def test_sismember_returns_false_after_spop(server: Server):
    c = server.make_client()
    val = c.send(ReqType.SADD, "set", "key1")
    val = c.send(ReqType.SPOP, "set")
    val = c.send(ReqType.SISMEMBER, "set", "key1")
    assert val is False


@test
def test_spop_returns_all_elements_when_called_repeatedly(server: Server):
    c = server.make_client()
    _ = c.send(ReqType.SADD, "set", "key1")
    _ = c.send(ReqType.SADD, "set", "key2")
    val1 = c.send(ReqType.SPOP, "set")
    assert isinstance(val1, bytes)
    val2 = c.send(ReqType.SPOP, "set")
    assert isinstance(val2, bytes)

    assert {val1, val2} == {b"key1", b"key2"}

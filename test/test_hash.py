from client import ReqType, RespType, resp_object_dict
from test_util import Server, test


@test
def test_hget_returns_value_after_hset(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.HSET, "map", "field", "value")
    assert res == RespType.OK
    res, val = c.send(ReqType.HGET, "map", "field")
    assert res == RespType.OK
    assert val == b"value"


@test
def test_hget_returns_value_after_latest_hset(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.HSET, "map", "field", "old-val")
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "field", "new-val")
    assert res == RespType.OK

    res, val = c.send(ReqType.HGET, "map", "field")
    assert res == RespType.OK
    assert val == b"new-val"


@test
def test_hget_missing_key(server: Server):
    c = server.make_client()
    res, val = c.send(ReqType.HGET, "map", "field")
    assert res == RespType.OK
    assert val is None


@test
def test_hget_missing_field(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.HSET, "map", "field1", "value")
    assert res == RespType.OK
    res, val = c.send(ReqType.HGET, "map", "field2")
    assert res == RespType.OK
    assert val is None


@test
def test_hlen_0_if_not_found(server: Server):
    c = server.make_client()
    res, val = c.send(ReqType.HLEN, "map")
    assert res == RespType.OK
    assert val == 0


@test
def test_hlen_err_if_not_hash(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.SET, "map", "projection")
    assert res == RespType.OK
    res, _val = c.send(ReqType.HLEN, "map")
    assert res == RespType.ERR


@test
def test_hlen_counts_unique_set_keys(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.HSET, "map", "pigs", 1)
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "cows", "nope")
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "farmers", 2)
    assert res == RespType.OK
    # Update shouldn't increase len
    res, _val = c.send(ReqType.HSET, "map", "pigs", 3)
    assert res == RespType.OK

    res, val = c.send(ReqType.HLEN, "map")
    assert res == RespType.OK
    assert val == 3


@test
def test_hlen_decreases_after_hdel(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.HSET, "map", "pigs", 1)
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "cows", "nope")
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "farmers", 2)
    assert res == RespType.OK
    # Update shouldn't increase len
    res, _val = c.send(ReqType.HDEL, "map", "pigs")
    assert res == RespType.OK

    res, val = c.send(ReqType.HLEN, "map")
    assert res == RespType.OK
    assert val == 2


@test
def test_hkeys_empty_if_not_found(server: Server):
    c = server.make_client()
    res, val = c.send(ReqType.HKEYS, "map")
    assert res == RespType.OK
    assert val == []


@test
def test_hkeys_err_if_not_hash(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.SET, "map", "projection")
    assert res == RespType.OK
    res, _val = c.send(ReqType.HKEYS, "map")
    assert res == RespType.ERR


@test
def test_hkeys_counts_unique_set_keys(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.HSET, "map", "pigs", 1)
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "cows", "nope")
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "farmers", 2)
    assert res == RespType.OK
    # Update shouldn't increase len
    res, _val = c.send(ReqType.HSET, "map", "pigs", 3)
    assert res == RespType.OK

    res, val = c.send(ReqType.HKEYS, "map")
    assert res == RespType.OK
    assert isinstance(val, list)
    assert set(val) == {b"pigs", b"cows", b"farmers"}


@test
def test_hkeys_decreases_after_hdel(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.HSET, "map", "pigs", 1)
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "cows", "nope")
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "farmers", 2)
    assert res == RespType.OK
    # Update shouldn't increase len
    res, _val = c.send(ReqType.HDEL, "map", "pigs")
    assert res == RespType.OK

    res, val = c.send(ReqType.HKEYS, "map")
    assert res == RespType.OK
    assert isinstance(val, list)
    assert set(val) == {b"cows", b"farmers"}


@test
def test_hgetall_0_if_not_found(server: Server):
    c = server.make_client()
    res, val = c.send(ReqType.HGETALL, "map")
    assert res == RespType.OK
    assert val == []


@test
def test_hgetall_err_if_not_hash(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.SET, "map", "projection")
    assert res == RespType.OK
    res, _val = c.send(ReqType.HGETALL, "map")
    assert res == RespType.ERR


@test
def test_hgetall_has_unique_set_keys(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.HSET, "map", "pigs", 1)
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "cows", "nope")
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "farmers", 2)
    assert res == RespType.OK
    # Update shouldn't increase len
    res, _val = c.send(ReqType.HSET, "map", "pigs", 3)
    assert res == RespType.OK

    res, val = c.send(ReqType.HGETALL, "map")
    assert res == RespType.OK
    assert isinstance(val, list)
    assert len(val) == 6
    assert resp_object_dict(val) == {b"pigs": 3, b"cows": b"nope", b"farmers": 2}


@test
def test_hgetall_decreases_after_hdel(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.HSET, "map", "pigs", 1)
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "cows", "nope")
    assert res == RespType.OK
    res, _val = c.send(ReqType.HSET, "map", "farmers", 2)
    assert res == RespType.OK
    # Update shouldn't increase len
    res, _val = c.send(ReqType.HDEL, "map", "pigs")
    assert res == RespType.OK

    res, val = c.send(ReqType.HGETALL, "map")
    assert res == RespType.OK
    assert isinstance(val, list)
    assert len(val) == 4
    assert resp_object_dict(val) == {b"cows": b"nope", b"farmers": 2}


@test
def test_hset_and_hget_10_000_keys(server: Server):
    n = 10_000
    c = server.make_client()

    for i in range(n):
        c.send_req(ReqType.HSET, "hash", f"key:{i}", f"value:{i}")
    for i in range(n):
        res, val = c.recv_resp()
        assert res == RespType.OK

    for i in range(n):
        c.send_req(ReqType.HGET, "hash", f"key:{i}")
    for i in range(n):
        res, val = c.recv_resp()
        assert res == RespType.OK
        assert val == f"value:{i}".encode("ascii")


@test
def test_get_returns_nil_after_deleting_hash(server: Server):
    c = server.make_client()
    res, _val = c.send(ReqType.HSET, "map", "field", "value")
    assert res == RespType.OK

    res, _val = c.send(ReqType.DEL, "map")
    assert res == RespType.OK

    res, val = c.send(ReqType.GET, "map")
    assert res == RespType.OK
    assert val is None

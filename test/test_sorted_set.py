import itertools

from client import Client, ResponseError, resp_object_dict, resp_object_pairs
from test_util import client_test


@client_test
def test_zscore_returns_value_after_zadd(c: Client):
    _ = c.send("ZADD", "scores", 4.32, "field")
    val = c.send("ZSCORE", "scores", "field")
    assert val == 4.32


@client_test
def test_zscore_returns_value_after_latest_zadd(c: Client):
    _ = c.send("ZADD", "scores", 4.32, "field")
    _ = c.send("ZADD", "scores", 5.9, "field")

    val = c.send("ZSCORE", "scores", "field")
    assert val == 5.9


@client_test
def test_zscore_missing_key(c: Client):
    val = c.send("ZSCORE", "scores", "field")
    assert val is None


@client_test
def test_zscore_missing_field(c: Client):
    _ = c.send("ZADD", "scores", 16.0, "field1")
    val = c.send("ZSCORE", "scores", "field2")
    assert val is None


@client_test
def test_zcard_0_if_not_found(c: Client):
    val = c.send("ZCARD", "scores")
    assert val == 0


@client_test
def test_zcard_err_if_not_hash(c: Client):
    _ = c.send("SET", "scores", "projection")
    try:
        _ = c.send("ZCARD", "scores")
    except ResponseError:
        return
    assert False, "Expected ResponseError"


@client_test
def test_zcard_counts_unique_set_keys(c: Client):
    _ = c.send("ZADD", "scores", 9.2, "pigs")
    _ = c.send("ZADD", "scores", 2.3, "cows")
    _ = c.send("ZADD", "scores", 1.6, "farmers")
    # Update shouldn't increase len
    _ = c.send("ZADD", "scores", 0.1, "pigs")

    val = c.send("ZCARD", "scores")
    assert val == 3


@client_test
def test_zcard_decreases_after_zrem(c: Client):
    _ = c.send("ZADD", "scores", 9.2, "pigs")
    _ = c.send("ZADD", "scores", 2.3, "cows")
    _ = c.send("ZADD", "scores", 1.6, "farmers")
    # Update shouldn't increase len
    _ = c.send("ZREM", "scores", "pigs")

    val = c.send("ZCARD", "scores")
    assert val == 2


@client_test
def test_zrank_nil_if_set_not_created(c: Client):
    val = c.send("ZRANK", "missing", "key")
    assert val is None


@client_test
def test_zrank_nil_if_member_missing(c: Client):
    _ = c.send("ZADD", "scores", 5.2, "key")
    val = c.send("ZRANK", "scores", "otherkey")
    assert val is None


@client_test
def test_zrank_ordered_by_score(c: Client):
    _ = c.send("ZADD", "scores", 9.2, "pigs")
    _ = c.send("ZADD", "scores", 1.6, "farmers")
    _ = c.send("ZADD", "scores", 2.3, "cows")

    val = c.send("ZRANK", "scores", "farmers")
    assert val == 0
    val = c.send("ZRANK", "scores", "cows")
    assert val == 1
    val = c.send("ZRANK", "scores", "pigs")
    assert val == 2


@client_test
def test_zrank_ordered_by_name_if_score_eq(c: Client):
    _ = c.send("ZADD", "scores", 1.6, "pigs")
    _ = c.send("ZADD", "scores", 1.6, "farmers")
    _ = c.send("ZADD", "scores", 2.3, "cows")

    val = c.send("ZRANK", "scores", "farmers")
    assert val == 0
    val = c.send("ZRANK", "scores", "pigs")
    assert val == 1
    val = c.send("ZRANK", "scores", "cows")
    assert val == 2


@client_test
def test_zrank_ordered_by_most_recent_score(c: Client):
    _ = c.send("ZADD", "scores", 9.2, "pigs")
    _ = c.send("ZADD", "scores", 1.6, "farmers")
    _ = c.send("ZADD", "scores", 2.3, "cows")

    _ = c.send("ZADD", "scores", 8.1, "farmers")

    val = c.send("ZRANK", "scores", "cows")
    assert val == 0
    val = c.send("ZRANK", "scores", "farmers")
    assert val == 1
    val = c.send("ZRANK", "scores", "pigs")
    assert val == 2


def create_numbers_set(c: Client, key: str, n: int):
    for i in range(n):
        _ = c.send_req("ZADD", key, float(i), str(i))

    for _ in range(n):
        _ = c.recv_resp()


@client_test
def test_zquery_all_ordered_by_score(c: Client):
    create_numbers_set(c, "numbers", 10)

    items = c.send("ZQUERY", "numbers", 0.0, "", 0, 100)
    print(items)
    assert items == [
        b"0",
        0.0,
        b"1",
        1.0,
        b"2",
        2.0,
        b"3",
        3.0,
        b"4",
        4.0,
        b"5",
        5.0,
        b"6",
        6.0,
        b"7",
        7.0,
        b"8",
        8.0,
        b"9",
        9.0,
    ]


@client_test
def test_zquery_with_limit_from_start(c: Client):
    create_numbers_set(c, "numbers", 10)

    items = c.send("ZQUERY", "numbers", 0.0, "", 0, 3)
    print(items)
    assert items == [b"0", 0.0, b"1", 1.0, b"2", 2.0]


@client_test
def test_zquery_with_offset_to_end(c: Client):
    create_numbers_set(c, "numbers", 10)

    items = c.send("ZQUERY", "numbers", 0.0, "", 5, 100)
    print(items)
    assert items == [
        b"5",
        5.0,
        b"6",
        6.0,
        b"7",
        7.0,
        b"8",
        8.0,
        b"9",
        9.0,
    ]


@client_test
def test_zquery_with_limit_and_offset_in_middle(c: Client):
    create_numbers_set(c, "numbers", 10)

    items = c.send("ZQUERY", "numbers", 0.0, "", 5, 3)
    print(items)
    assert items == [
        b"5",
        5.0,
        b"6",
        6.0,
        b"7",
        7.0,
    ]


@client_test
def test_zquery_with_score_from_middle(c: Client):
    create_numbers_set(c, "numbers", 10)

    items = c.send("ZQUERY", "numbers", 4.2, "", 1, 3)
    print(items)
    assert items == [
        b"6",
        6.0,
        b"7",
        7.0,
        b"8",
        8.0,
    ]


@client_test
def test_zadd_zscore_del_10_000_keys(c: Client):
    n = 10_000

    for i in range(n):
        # Store the hash so that keys aren't inserted in order
        c.send_req("ZADD", "scores", float(hash(i)), f"key:{i}")
    for i in range(n):
        val = c.recv_resp()

    for i in range(n):
        c.send_req("ZSCORE", "scores", f"key:{i}")
    for i in range(n):
        val = c.recv_resp()
        assert val == float(hash(i))

    val = c.send("DEL", "scores")
    assert val is True

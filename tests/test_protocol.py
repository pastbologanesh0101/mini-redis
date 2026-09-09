import pytest

from miniredis.protocol import (
    NeedMoreData,
    Reader,
    RespError,
    SimpleString,
    encode,
)


def test_parse_array_of_bulk_strings():
    r = Reader()
    r.feed(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
    assert r.read() == [b"GET", b"foo"]


def test_parse_incremental():
    r = Reader()
    r.feed(b"*1\r\n$4\r\nPI")
    with pytest.raises(NeedMoreData):
        r.read()
    r.feed(b"NG\r\n")
    assert r.read() == [b"PING"]


def test_parse_multiple_commands_in_one_buffer():
    r = Reader()
    r.feed(b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n")
    assert list(r) == [[b"PING"], [b"ECHO", b"hi"]]


def test_parse_nil_and_integers():
    r = Reader()
    r.feed(b":42\r\n$-1\r\n*-1\r\n")
    assert r.read() == 42
    assert r.read() is None
    assert r.read() is None


def test_parse_inline_command():
    r = Reader()
    r.feed(b"PING\r\n")
    assert r.read() == [b"PING"]


def test_parse_nested_array():
    r = Reader()
    r.feed(b"*2\r\n*1\r\n:1\r\n$2\r\nok\r\n")
    assert r.read() == [[1], b"ok"]


@pytest.mark.parametrize("value,expected", [
    (None, b"$-1\r\n"),
    (SimpleString("OK"), b"+OK\r\n"),
    (RespError("ERR bad"), b"-ERR bad\r\n"),
    (0, b":0\r\n"),
    (-5, b":-5\r\n"),
    (b"hello", b"$5\r\nhello\r\n"),
    (b"", b"$0\r\n\r\n"),
    (["a", 1, None], b"*3\r\n$1\r\na\r\n:1\r\n$-1\r\n"),
])
def test_encode(value, expected):
    assert encode(value) == expected


def test_roundtrip():
    r = Reader()
    r.feed(encode([b"SET", b"k", b"v"]))
    assert r.read() == [b"SET", b"k", b"v"]

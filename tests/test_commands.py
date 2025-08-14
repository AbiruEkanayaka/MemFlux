from .common import send_resp_command, assert_eq

def test_byte(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    print("== Byte tests ==")
    assert_eq(send(["SET", "foo", "bar"]), "+OK", "SET foo bar")
    assert_eq(send(["GET", "foo"]), "$3\r\nbar", "GET foo")
    assert_eq(send(["DELETE", "foo"]), ":1", "DELETE foo")
    assert_eq(send(["GET", "foo"]), "$-1", "GET deleted foo")

def test_json(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    print("== JSON tests ==")
    # The server returns the correct JSON, but the RESP bulk string length is by bytes.
    # '{"a":1,"b":2}' is 13 bytes, '{"a":1,"b":2,"c":3}' is 19 bytes.
    assert_eq(send(["JSON.SET", "j1", '{"a":1,"b":2}']), "+OK", "JSON.SET j1")
    assert_eq(send(["JSON.GET", "j1"]), "$13\r\n{\"a\":1,\"b\":2}", "JSON.GET j1")
    # JSON.GET j1.a returns $1\r\n1
    assert_eq(send(["JSON.GET", "j1.a"]), "$1\r\n1", "JSON.GET j1.a")
    assert_eq(send(["JSON.SET", "j1.c", "3"]), "+OK", "JSON.SET j1.c")
    assert_eq(send(["JSON.GET", "j1"]), "$19\r\n{\"a\":1,\"b\":2,\"c\":3}", "JSON.GET j1 after set c")
    assert_eq(send(["DELETE", "j1"]), ":1", "DELETE j1")

def test_lists(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    print("== List tests ==")
    assert_eq(send(["LPUSH", "l1", "a", "b"]), ":2", "LPUSH l1 a b")
    assert_eq(send(["RPUSH", "l1", "c"]), ":3", "RPUSH l1 c")
    assert_eq(send(["LPOP", "l1"]), "$1\r\nb", "LPOP l1 (pushed b then a, so b is at the head)")
    assert_eq(send(["RPOP", "l1"]), "$1\r\nc", "RPOP l1")
    assert_eq(send(["LRANGE", "l1", "0", "10"]), "*1\r\n$1\r\na", "LRANGE l1 0 10")
    assert_eq(send(["DELETE", "l1"]), ":1", "DELETE l1")


def test_sets(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    print("== Set tests ==")
    assert_eq(send(["SADD", "set1", "a", "b", "c"]), ":3", "SADD set1 a b c")
    assert_eq(send(["SADD", "set1", "b", "d"]), ":1", "SADD set1 b d")
    assert_eq(send(["SISMEMBER", "set1", "a"]), ":1", "SISMEMBER set1 a")
    assert_eq(send(["SISMEMBER", "set1", "z"]), ":0", "SISMEMBER set1 z")
    out = send(["SMEMBERS", "set1"])
    lines = out.splitlines()
    assert_eq(lines[0], "*4", "SMEMBERS set1 count")
    members = set(lines[2::2]) # Simplified parsing for this specific test
    assert_eq(members, {"a", "b", "c", "d"}, "SMEMBERS set1 content")
    assert_eq(send(["SREM", "set1", "b", "x"]), ":1", "SREM set1 b x")
    out = send(["SMEMBERS", "set1"])
    lines = out.splitlines()
    members = set(lines[2::2])
    assert_eq(members, {"a", "c", "d"}, "SMEMBERS set1 after SREM")
    assert_eq(send(["DELETE", "set1"]), ":1", "DELETE set1")









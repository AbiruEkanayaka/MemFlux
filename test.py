import socket
import sys
import shlex
import time
import statistics
import json

# Add prompt_toolkit for better interactive input
try:
    from prompt_toolkit import PromptSession
except ImportError:
    PromptSession = None

def send_resp_command(sock, parts):
    resp = f"*{len(parts)}\r\n"
    for p in parts:
        p_bytes = p.encode('utf-8')
        resp += f"${len(p_bytes)}\r\n"
        resp += p
        resp += "\r\n"
    t0 = time.perf_counter()
    try:
        sock.sendall(resp.encode('utf-8'))
    except BrokenPipeError:
        print("[ERROR] Connection closed by server (Broken pipe). The server may have crashed or exited.")
        sys.exit(1)
    t1 = time.perf_counter()
    # Read the full response (handle large/multi-line responses)
    response = b""
    while True:
        try:
            chunk = sock.recv(4096)
        except BrokenPipeError:
            print("[ERROR] Connection closed by server during recv (Broken pipe).")
            sys.exit(1)
        if not chunk:
            break
        response += chunk
        # Heuristic: if we see a line ending with \r\n and no more data, break
        if len(chunk) < 4096:
            break
        # For multi-bulk, we could parse the protocol, but for now, just read until no more data
    t2 = time.perf_counter()
    send_time = (t1 - t0) * 1000
    latency = (t2 - t1) * 1000
    total = (t2 - t0) * 1000
    return response.decode('utf-8', errors='replace'), send_time, latency, total

def benchmark(sock, parts, count):
    send_times = []
    latencies = []
    totals = []
    for i in range(count):
        _, send, lat, tot = send_resp_command(sock, parts)
        send_times.append(send)
        latencies.append(lat)
        totals.append(tot)
    print(f"Benchmark results for {count} requests:")
    print(f"  Send time:      avg={statistics.mean(send_times):.2f}ms min={min(send_times):.2f}ms max={max(send_times):.2f}ms")
    print(f"  Latency:        avg={statistics.mean(latencies):.2f}ms min={min(latencies):.2f}ms max={max(latencies):.2f}ms")
    print(f"  Total roundtrip avg={statistics.mean(totals):.2f}ms min={min(totals):.2f}ms max={max(totals):.2f}ms")

def unit_test(sock, mode):
    def send(parts):
        resp, *_ = send_resp_command(sock, parts)
        return resp.strip()

    def assert_eq(actual, expected, desc):
        if actual == expected:
            print(f"[PASS] {desc}")
        else:
            print(f"[FAIL] {desc}\n  Expected: {expected!r}\n  Actual:   {actual!r}")

    def test_byte():
        print("== Byte tests ==")
        assert_eq(send(["SET", "foo", "bar"]), "+OK", "SET foo bar")
        assert_eq(send(["GET", "foo"]), "$3\r\nbar", "GET foo")
        assert_eq(send(["DELETE", "foo"]), ":1", "DELETE foo")
        assert_eq(send(["GET", "foo"]), "$-1", "GET deleted foo")

    def test_json():
        print("== JSON tests ==")
        # The server returns the correct JSON, but the RESP bulk string length is by bytes.
        # '{"a":1,"b":2}' is 13 bytes, '{"a":1,"b":2,"c":3}' is 19 bytes.
        assert_eq(send(["JSON.SET", "j1", '{"a":1,"b":2}']), "+OK", "JSON.SET j1")
        assert_eq(send(["JSON.GET", "j1"]), '$13\r\n{"a":1,"b":2}', "JSON.GET j1")
        # JSON.GET j1.a returns $1\r\n1
        assert_eq(send(["JSON.GET", "j1.a"]), "$1\r\n1", "JSON.GET j1.a")
        assert_eq(send(["JSON.SET", "j1.c", "3"]), "+OK", "JSON.SET j1.c")
        assert_eq(send(["JSON.GET", "j1"]), '$19\r\n{"a":1,"b":2,"c":3}', "JSON.GET j1 after set c")
        assert_eq(send(["DELETE", "j1"]), ":1", "DELETE j1")

    def test_lists():
        print("== List tests ==")
        # LPUSH l1 a b: pushes b then a, so list is [b, a]
        assert_eq(send(["LPUSH", "l1", "a", "b"]), ":2", "LPUSH l1 a b")
        # RPUSH l1 c: list is [b, a, c]
        assert_eq(send(["RPUSH", "l1", "c"]), ":3", "RPUSH l1 c")
        # LPOP l1: removes a (leftmost, because LPUSH pushes leftmost)
        assert_eq(send(["LPOP", "l1"]), "$1\r\na", "LPOP l1")
        # RPOP l1: removes c (rightmost)
        assert_eq(send(["RPOP", "l1"]), "$1\r\nc", "RPOP l1")
        # LRANGE l1 0 10: should return only b
        assert_eq(send(["LRANGE", "l1", "0", "10"]), "*1\r\n$1\r\nb", "LRANGE l1 0 10")
        assert_eq(send(["DELETE", "l1"]), ":1", "DELETE l1")

    def test_sql():
        print("== SQL tests ==")
        send(["JSON.SET", "user:1", '{"profile":{"name":"Alice","age":30,"city":"London"}}'])
        send(["JSON.SET", "user:2", '{"profile":{"name":"Bob","age":25,"city":"Paris"}}'])
        send(["JSON.SET", "user:3", '{"profile":{"name":"Carol","age":30,"city":"London"}}'])
        send(["JSON.SET", "user:4", '{"profile":{"name":"Dave","age":40,"city":"Berlin"}}'])
        send(["JSON.SET", "user:5", '{"profile":{"name":"Eve","age":35,"city":"Paris"}}'])
        send(["CREATEINDEX", "user:*", "ON", "profile.age"])
        send(["CREATEINDEX", "user:*", "ON", "profile.city"])
        out = send(["SQL", "SELECT profile.name FROM user WHERE profile.age = 30"])
        # Accept either RESP array or JSON array as bulk string
        if out.startswith("$"):
            # Bulk string with JSON array
            json_str = out.split("\r\n", 1)[1]
            try:
                arr = json.loads(json_str)
                names = sorted([row["profile"]["name"] for row in arr])
            except Exception:
                names = []
            assert_eq(len(names), 2, "SQL SELECT profile.name WHERE age=30 (row count)")
            assert_eq(names, ["Alice", "Carol"], "SQL SELECT profile.name WHERE age=30 (names)")
        else:
            # RESP array
            lines = out.splitlines()
            assert_eq(lines[0], "*2", "SQL SELECT profile.name WHERE age=30 (row count)")
            names = []
            i = 1
            while i < len(lines):
                if lines[i].startswith("$"):
                    names.append(json.loads(lines[i+1]))
                    i += 2
                else:
                    i += 1
            names = sorted(names)
            assert_eq(names, ["Alice", "Carol"], "SQL SELECT profile.name WHERE age=30 (names)")
        out = send(["SQL", "SELECT COUNT(*) FROM user"])
        if out.startswith("$"):
            json_str = out.split("\r\n", 1)[1]
            try:
                arr = json.loads(json_str)
                count_val = arr[0].get('__Count([1])', None)
            except Exception:
                count_val = None
            assert_eq(count_val, 5, "SQL COUNT(*) == 5")
        else:
            lines = out.splitlines()
            assert_eq(lines[0], "*1", "SQL COUNT(*)")
            count_val = None
            for i in range(1, len(lines)):
                if lines[i].startswith("$") and i+1 < len(lines):
                    try:
                        count_val = int(json.loads(lines[i+1]))
                    except Exception:
                        pass
            assert_eq(count_val, 5, "SQL COUNT(*) == 5")
        out = send(["SQL", "SELECT profile.city, COUNT(*) FROM user GROUP BY profile.city"])
        if out.startswith("$"):
            json_str = out.split("\r\n", 1)[1]
            try:
                arr = json.loads(json_str)
                group_count = len(arr)
            except Exception:
                group_count = None
            assert_eq(group_count, 3, "SQL GROUP BY city (3 cities)")
        else:
            lines = out.splitlines()
            assert_eq(lines[0], "*3", "SQL GROUP BY city (3 cities)")
        # Clean up
        for i in range(1, 6):
            send(["DELETE", f"user:{i}"])

    if mode in ("byte", "all"):
        test_byte()
    if mode in ("json", "all"):
        test_json()
    if mode in ("lists", "all"):
        test_lists()
    if mode in ("sql", "all"):
        test_sql()

if __name__ == "__main__":
    host = "127.0.0.1"
    port = 6380
    try:
        sock = socket.create_connection((host, port))
        print("Connected to the db")
    except Exception as e:
        print(f"Failed to connect to the db: {e}")
        sys.exit(1)
    try:
        if len(sys.argv) > 1:
            if sys.argv[1] == "bench":
                # Usage: python test.py bench COUNT COMMAND [ARGS...]
                if len(sys.argv) < 4:
                    print("Usage: python test.py bench COUNT COMMAND [ARGS...]")
                    sys.exit(1)
                count = int(sys.argv[2])
                parts = shlex.split(" ".join(sys.argv[3:]))
                benchmark(sock, parts, count)
            elif sys.argv[1] == "unit":
                # Usage: python test.py unit {sql,json,byte,lists,all}
                if len(sys.argv) < 3:
                    print("Usage: python test.py unit {sql,json,byte,lists,all}")
                    sys.exit(1)
                unit_test(sock, sys.argv[2].lower())
            else:
                # Parse command line as a single command
                # If the command is SQL, join all arguments after "SQL" into one argument
                if sys.argv[1].upper() == "SQL":
                    parts = [sys.argv[1], " ".join(sys.argv[2:])]
                else:
                    parts = shlex.split(" ".join(sys.argv[1:]))
                resp, send, lat, tot = send_resp_command(sock, parts)
                print(resp)
                print(f"Send: {send:.2f}ms, Latency: {lat:.2f}ms, Total: {tot:.2f}ms")
        else:
            # Interactive mode: read lines from stdin or prompt_toolkit
            print("Enter commands (e.g. SET user:123 '{\"profile\":{\"full name\":\"Jane Doe\"}}'):")
            if PromptSession is not None:
                session = PromptSession()
                while True:
                    try:
                        line = session.prompt("> ")
                    except (EOFError, KeyboardInterrupt):
                        print()
                        break
                    line = line.strip()
                    if not line:
                        continue
                    # If the command is SQL, join everything after SQL as one argument
                    if line.upper().startswith("SQL "):
                        cmd, rest = line.split(" ", 1)
                        parts = [cmd, rest]
                    else:
                        parts = shlex.split(line)
                    resp, send, lat, tot = send_resp_command(sock, parts)
                    print(resp)
                    print(f"Send: {send:.2f}ms, Latency: {lat:.2f}ms, Total: {tot:.2f}ms")
            else:
                # Fallback: plain stdin
                for line in sys.stdin:
                    line = line.strip()
                    if not line:
                        continue
                    # If the command is SQL, join everything after SQL as one argument
                    if line.upper().startswith("SQL "):
                        cmd, rest = line.split(" ", 1)
                        parts = [cmd, rest]
                    else:
                        parts = shlex.split(line)
                    resp, send, lat, tot = send_resp_command(sock, parts)
                    print(resp)
                    print(f"Send: {send:.2f}ms, Latency: {lat:.2f}ms, Total: {tot:.2f}ms")
    finally:
        sock.close()

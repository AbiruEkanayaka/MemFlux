from .common import send_resp_command, assert_eq

def test_wrongtype_errors(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    print("== WRONGTYPE Error Test Suite ==")

    # --- 1. Setup: Create one key of each major type ---
    print("\n-- Phase 1: Setup --")
    send(["DELETE", "string_key", "list_key", "set_key", "json_key"])
    send(["SET", "string_key", "hello"])
    send(["LPUSH", "list_key", "a"])
    send(["SADD", "set_key", "a"])
    send(["JSON.SET", "json_key", '{"a":1}'])
    print("[PASS] Setup complete")

    # --- 2. Test String Commands on Wrong Types ---
    print("\n-- Phase 2: String Command Errors --")
    assert_eq(send(["GET", "list_key"]).startswith("-ERR WRONGTYPE"), True, "GET on a List")
    assert_eq(send(["GET", "set_key"]).startswith("-ERR WRONGTYPE"), True, "GET on a Set")
    # GET on JSON is a special case that serializes it, so no error is expected.

    # --- 3. Test List Commands on Wrong Types ---
    print("\n-- Phase 3: List Command Errors --")
    assert_eq(send(["LPUSH", "string_key", "x"]).startswith("-ERR WRONGTYPE"), True, "LPUSH on a String")
    assert_eq(send(["LPUSH", "set_key", "x"]).startswith("-ERR WRONGTYPE"), True, "LPUSH on a Set")
    assert_eq(send(["LPUSH", "json_key", "x"]).startswith("-ERR WRONGTYPE"), True, "LPUSH on a JSON object")

    # --- 4. Test Set Commands on Wrong Types ---
    print("\n-- Phase 4: Set Command Errors --")
    assert_eq(send(["SADD", "string_key", "x"]).startswith("-ERR WRONGTYPE"), True, "SADD on a String")
    assert_eq(send(["SADD", "list_key", "x"]).startswith("-ERR WRONGTYPE"), True, "SADD on a List")
    assert_eq(send(["SADD", "json_key", "x"]).startswith("-ERR WRONGTYPE"), True, "SADD on a JSON object")

    # --- 5. Test JSON Commands on Wrong Types ---
    print("\n-- Phase 5: JSON Command Errors --")
    assert_eq(send(["JSON.GET", "string_key"]).startswith("-ERR WRONGTYPE"), True, "JSON.GET on a String")
    assert_eq(send(["JSON.GET", "list_key"]).startswith("-ERR WRONGTYPE"), True, "JSON.GET on a List")
    assert_eq(send(["JSON.GET", "set_key"]).startswith("-ERR WRONGTYPE"), True, "JSON.GET on a Set")

    # --- 6. Cleanup ---
    print("\n-- Phase 6: Cleanup --")
    send(["DELETE", "string_key", "list_key", "set_key", "json_key"])
    print("[PASS] WRONGTYPE tests complete")

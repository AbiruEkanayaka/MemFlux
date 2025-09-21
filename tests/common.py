import socket
import sys
import time
import json
import ssl
import os
import shlex

# Add libs/python to path to import memflux
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'libs', 'python')))
try:
    import libs.python.memflux as memflux
    _FFIResponseType = memflux._FFIResponseType
    FFI_ENABLED = True
except ImportError:
    FFI_ENABLED = False
    # Define dummy classes if import fails, so the script doesn't crash immediately
    class _FFIResponseType:
        OK = 0
        SIMPLE_STRING = 1
        BYTES = 2
        MULTI_BYTES = 3
        INTEGER = 4
        NIL = 5
        ERROR = 6


# --- Test Runner ---
class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.failed_tests = []

    def record_pass(self, desc):
        self.passed += 1
        print(f"[PASS] {desc}")

    def record_fail(self, actual, expected, desc):
        self.failed += 1
        self.failed_tests.append({'desc': desc, 'expected': expected, 'actual': actual})
        print(f"[FAIL] {desc}")

    def summary(self):
        print("\n--- Unit Test Summary ---")
        if self.failed == 0 and self.passed > 0:
            print(f"All {self.passed} unit tests passed!")
        elif self.failed == 0 and self.passed == 0:
            print("No tests were run.")
        else:
            total = self.passed + self.failed
            print(f"{self.passed}/{total} unit tests passed, {self.failed} failed.")
            if self.failed_tests:
                print("\nFailed tests:")
                for failure in self.failed_tests:
                    print(f"- {failure['desc']}")
                    print(f"  - Expected: {failure['expected']!r}")
                    print(f"  - Actual:   {failure['actual']!r}")
        print("--- End of Summary ---")
        if self.failed > 0:
            sys.exit(1)


# Global instance to be used by all test modules
test_result = TestResult()


def create_connection(retry=False, ffi_path=None):
    if ffi_path:
        if not FFI_ENABLED:
            print("[ERROR] FFI mode requested, but 'memflux' library could not be imported.")
            sys.exit(1)
        if not retry: print(f"Connecting via FFI: {ffi_path}")
        config = {}
        try:
            with open('config.json', 'r') as f:
                config = json.load(f)
        except FileNotFoundError:
            if not retry: print("config.json not found, using default FFI config.")
            config = {
                "wal_file": "memflux.wal",
                "wal_overflow_file": "memflux.wal.overflow",
                "snapshot_file": "memflux.snapshot",
                "snapshot_temp_file": "memflux.snapshot.tmp",
                "wal_size_threshold_mb": 128,
                "maxmemory_mb": 0,
                "eviction_policy": "lru"
            }
        
        try:
            ffi_conn = memflux.connect(config, ffi_path)
            if not retry: print("Connected via FFI.")
            return ffi_conn, None
        except (memflux.InterfaceError, memflux.DatabaseError) as e:
            if not retry: 
                print(f"Failed to connect via FFI: {e}")
                sys.exit(1)
            return None, None

    host = "127.0.0.1"
    port = 8360
    sock = None

    config = {}
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        if not retry:
            print("config.json not found, proceeding with defaults.")

    try:
        sock = socket.create_connection((host, port))

        if config.get('encrypt'):
            if not retry: print("Encryption enabled, wrapping socket...")
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            sock = context.wrap_socket(sock, server_hostname=host)
            if not retry: print("Socket wrapped with TLS.")

        reader = sock.makefile('rb')
        if not retry: print("Connected to the db")

        password = config.get('requirepass')
        if password and password != "":
            if not retry: print("Password required, sending AUTH command.")
            resp, _, _, _ = send_resp_command(sock, reader, ["AUTH", password])
            if "OK" not in resp:
                if not retry:
                    print(f"Authentication failed: {resp.strip()}")
                sock.close()
                if not retry:
                    sys.exit(1)
                else:
                    return None, None
            if not retry: print("Authentication successful.")
        
        return sock, reader

    except Exception as e:
        if not retry:
            print(f"Failed to connect or authenticate: {e}")
        if sock:
            sock.close()
        if not retry:
            sys.exit(1)
        else:
            return None, None


def read_resp_response(reader):
    first_byte = reader.read(1)
    if not first_byte:
        print("[ERROR] Connection closed by server during read.")
        sys.exit(1)

    line = reader.readline()

    if first_byte in (b'+', b'-', b':'):
        return first_byte + line
    elif first_byte == b'$':
        length = int(line.strip())
        if length == -1:
            return b'$-1\r\n'
        data = reader.read(length + 2)
        return b'$' + line + data
    elif first_byte == b'*':
        count = int(line.strip())
        if count == -1:
            return b'*-1\r\n'
        
        full_response_parts = [b'*' + line]
        for _ in range(count):
            element_first_byte = reader.read(1)
            element_line = reader.readline()
            full_response_parts.append(element_first_byte + element_line)
            
            if element_first_byte == b'$':
                element_length = int(element_line.strip())
                if element_length > -1:
                    element_data = reader.read(element_length + 2)
                    full_response_parts.append(element_data)

        return b''.join(full_response_parts)
    else:
        raise ValueError(f"Unknown RESP type byte: {first_byte}")


def format_ffi_response_as_resp(response_type, result_list):
    if response_type is None:
        return b''

    if response_type == _FFIResponseType.OK:
        return b"+OK\r\n"
    elif response_type == _FFIResponseType.SIMPLE_STRING:
        val = result_list[0] if result_list else ""
        return f"+{val}\r\n".encode('utf-8')
    elif response_type == _FFIResponseType.INTEGER:
        val = result_list[0] if result_list else 0
        return f":{val}\r\n".encode('utf-8')
    elif response_type == _FFIResponseType.NIL:
        return b"$-1\r\n"
    elif response_type == _FFIResponseType.BYTES:
        if not result_list:
            return b"$0\r\n\r\n"
        val = result_list[0]
        if isinstance(val, bytes):
            val_bytes = val
        else:
            val_bytes = json.dumps(val, separators=(',', ':')).encode('utf-8')
        return f"${len(val_bytes)}\r\n".encode('utf-8') + val_bytes + b"\r\n"
    elif response_type == _FFIResponseType.MULTI_BYTES:
        response = f"*{len(result_list)}\r\n".encode('utf-8')
        for item in result_list:
            if isinstance(item, bytes):
                item_bytes = item
            else:
                item_bytes = json.dumps(item, separators=(',', ':')).encode('utf-8')
            response += f"${len(item_bytes)}\r\n".encode('utf-8')
            response += item_bytes
            response += b"\r\n"
        return response
    else:
        return b"-ERR Unhandled FFI response type\r\n"


def send_resp_command(conn, reader, parts):
    if reader is not None: # Socket mode
        sock = conn
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
        
        try:
            response_bytes = read_resp_response(reader)
        except Exception as e:
            print(f"[ERROR] Failed to read/parse RESP response: {e}")
            sys.exit(1)
            
        t2 = time.perf_counter()
        send_time = (t1 - t0) * 1000
        latency = (t2 - t1) * 1000
        total = (t2 - t0) * 1000
        return response_bytes.decode('utf-8', errors='replace'), send_time, latency, total
    else: # FFI mode
        # In FFI mode, 'conn' is the cursor object itself.
        cursor = conn
        if parts[0].upper() == 'SQL':
            command_str = " ".join(parts)
        else:
            command_str = shlex.join(parts)
        
        t0 = time.perf_counter()
        try:
            cursor.execute(command_str)
            t1 = time.perf_counter()
            
            response_type = cursor._last_result_type
            result_list = cursor._last_result
            
            response_bytes = format_ffi_response_as_resp(response_type, result_list)
            
            t2 = time.perf_counter()
            exec_time = (t1 - t0) * 1000
            total_time = (t2 - t0) * 1000
            
            # The cursor must be closed by the test function that created it.
            return response_bytes.decode('utf-8', errors='replace'), exec_time, 0, total_time

        except memflux.DatabaseError as e:
            t1 = time.perf_counter()
            response_str = f"-ERR {e}\r\n"
            t2 = time.perf_counter()
            exec_time = (t1 - t0) * 1000
            total_time = (t2 - t0) * 1000
            return response_str, exec_time, 0, total_time

def assert_eq(actual, expected, desc):
    if actual == expected:
        test_result.record_pass(desc)
    else:
        test_result.record_fail(actual, expected, desc)

def extract_json_from_bulk(resp):
    if resp.startswith(b"$"):
        try:
            parts = resp.split(b'\r\n', 1)
            if len(parts) < 2:
                raise ValueError("Incomplete RESP bulk string format")
            
            length_str = parts[0][1:]
            length = int(length_str)
            
            if length == -1:
                return None
            
            json_str_raw = parts[1]
            if json_str_raw.endswith(b'\r\n'):
                json_str_raw = json_str_raw[:-2]

            return json.loads(json_str_raw.decode('utf-8'))
        except Exception as e:
            print(f"[ERROR] Failed to extract JSON from {resp!r}: {e}")
            return None
    return None

import socket
import sys
import time
import json

def read_resp_response(reader):
    """
    Reads a full RESP response from the given file-like reader object.
    """
    first_byte = reader.read(1)
    if not first_byte:
        # Connection closed by server
        print("[ERROR] Connection closed by server during read.")
        sys.exit(1)

    line = reader.readline()

    if first_byte in (b'+', b'-', b':'): # Simple String, Error, Integer
        return first_byte + line
    elif first_byte == b'$': # Bulk String
        length = int(line.strip())
        if length == -1:
            return b'$-1\r\n'
        # Read the data itself + the trailing \r\n
        data = reader.read(length + 2)
        return b'$' + line + data
    elif first_byte == b'*': # Array
        count = int(line.strip())
        if count == -1:
            return b'*-1\r\n'
        
        # Reconstruct the raw string by reading each part of the array
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


def send_resp_command(sock, reader, parts):
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
    
    # Read the full response using the proper RESP parser
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

def assert_eq(actual, expected, desc):
    if actual == expected:
        print(f"[PASS] {desc}")
    else:
        print(f"[FAIL] {desc}\n  Expected: {expected!r}\n  Actual:   {actual!r}")

def extract_json_from_bulk(resp):
    """Extract JSON payload from RESP bulk string ($len\r\n...json...)"""
    if resp.startswith(b"$"):
        try:
            # Find the first \r\n to get the length part, then the actual data.
            # Use splitlines(keepends=True) to retain \r\n for accurate parsing.
            parts = resp.split(b'\r\n', 1)
            if len(parts) < 2:
                raise ValueError("Incomplete RESP bulk string format")
            
            length_str = parts[0][1:] # Remove the $
            length = int(length_str)
            
            if length == -1:
                return None # Represents a null bulk string
            
            # The actual data is the second part
            json_str_raw = parts[1]
            # Remove the trailing \r\n if present, assuming the data itself does not contain it
            # For `read_resp_response`, the data includes the trailing \r\n
            # so we need to ensure it's removed before JSON parsing.
            if json_str_raw.endswith(b'\r\n'):
                json_str_raw = json_str_raw[:-2]

            return json.loads(json_str_raw.decode('utf-8'))
        except Exception as e:
            print(f"[ERROR] Failed to extract JSON from {resp!r}: {e}")
            return None
    return None









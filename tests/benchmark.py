import statistics
import time
from .common import send_resp_command, read_resp_response

def benchmark(sock, reader, parts, count):
    send_times = []
    latencies = []
    totals = []
    for i in range(count):
        _, send, lat, tot = send_resp_command(sock, reader, parts)
        send_times.append(send)
        latencies.append(lat)
        totals.append(tot)
    print(f"Benchmark results for {count} requests:")
    print(f"  Send time:      avg={statistics.mean(send_times):.2f}ms min={min(send_times):.2f}ms max={max(send_times):.2f}ms")
    print(f"  Latency:        avg={statistics.mean(latencies):.2f}ms min={min(latencies):.2f}ms max={max(latencies):.2f}ms")
    print(f"  Total roundtrip avg={statistics.mean(totals):.2f}ms min={min(totals):.2f}ms max={max(totals):.2f}ms")


def benchmark_ops_sec(sock, reader, parts, duration_sec, pipeline_size=1000):
    """
    Benchmarks operations per second for a given command using pipelining.
    """
    print(f"Benchmarking ops/sec for {duration_sec} seconds (pipeline size: {pipeline_size})...")
    
    # Pre-encode the command to send
    resp = f"*{len(parts)}\r\n"
    for p in parts:
        p_bytes = p.encode('utf-8')
        resp += f"${len(p_bytes)}\r\n"
        resp += p
        resp += "\r\n"
    command_bytes = resp.encode('utf-8')

    # Send one command to make sure connection is ok and warm up
    try:
        sock.sendall(command_bytes)
        read_resp_response(reader)
    except Exception as e:
        print(f"Initial command failed, aborting benchmark: {e}")
        return

    start_time = time.time()
    ops_count = 0
    
    while time.time() - start_time < duration_sec:
        # Send a pipeline of commands
        pipeline_data = command_bytes * pipeline_size
        try:
            sock.sendall(pipeline_data)
        except BrokenPipeError:
            print("[ERROR] Connection closed by server (Broken pipe).")
            break
            
        # Read responses for the pipeline
        try:
            for _ in range(pipeline_size):
                read_resp_response(reader)
            ops_count += pipeline_size
        except Exception as e:
            print(f"[ERROR] Failed to read/parse RESP response during benchmark: {e}")
            # After a read error, the stream is likely out of sync.
            # It's better to stop than to report incorrect numbers.
            break

    end_time = time.time()
    # The actual benchmark duration might be slightly different
    actual_duration = end_time - start_time
    ops_per_sec = ops_count / actual_duration if actual_duration > 0 else 0

    print("\n--- Ops/sec Benchmark ---")
    print(f"  Pipeline size:    {pipeline_size}")
    print(f"  Total operations: {ops_count}")
    print(f"  Total time:       {actual_duration:.2f} seconds")
    print(f"  Operations/sec:   {ops_per_sec:,.2f}")



def fill_db(sock, reader, mbs):
    """
    Fills the database with a specified amount of data in MB.
    """
    print(f"Filling database with {mbs} MB of data...")
    
    # Use 256KB values
    value_size_kb = 256
    large_value = 'x' * (value_size_kb * 1024)
    
    num_writes = (mbs * 1024) // value_size_kb
    
    if num_writes == 0:
        print("MB value is too low to write any key. Minimum is 1 write of 256KB.")
        return

    print(f"Will perform {num_writes} writes of {value_size_kb}KB each.")

    start_time = time.time()
    for i in range(num_writes):
        key = f"fill_key:{i}"
        send_resp_command(sock, reader, ["SET", key, large_value])
        if (i + 1) % 10 == 0 or i == num_writes -1:
            print(f"  ... {i+1}/{num_writes} writes completed.", end='\r')
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\n\nFinished filling database.")
    print(f"  Total writes: {num_writes}")
    print(f"  Total data:   ~{mbs} MB")
    print(f"  Duration:     {duration:.2f} seconds")
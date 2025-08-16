import sys
import shlex

from tests.common import send_resp_command, create_connection
from tests.test_commands import test_byte, test_json, test_lists, test_sets
from tests.test_sql import test_sql
from tests.test_aliases import test_sql_aliases
from tests.test_persistence import test_snapshot
from tests.test_types import test_type_casting
from tests.test_schema_validation import test_schema_validation_and_sorting
from tests.test_case import test_case
from tests.test_like import test_like
from tests.test_subqueries import test_subqueries
from tests.test_union import test_union
from tests.test_functions import test_string_functions, test_numeric_functions, test_datetime_functions, test_cast_function
from tests.benchmark import benchmark, benchmark_ops_sec
from tests.test_advanced_commands import test_advanced_commands
from tests.test_sql_operators import test_sql_comparison_operators
from tests.test_indexing import test_index_maintenance
from tests.test_recovery import test_recovery
from tests.test_wrongtype_errors import test_wrongtype_errors


# Add prompt_toolkit for better interactive input
try:
    from prompt_toolkit import PromptSession
except ImportError:
    PromptSession = None

def unit_test(sock, reader, mode):
    # Existing tests
    if mode in ("byte", "all"):
        test_byte(sock, reader)
    if mode in ("json", "all"):
        test_json(sock, reader)
    if mode in ("lists", "all"):
        test_lists(sock, reader)
    if mode in ("sets", "all"):
        test_sets(sock, reader)
    if mode in ("sql", "all"):
        test_sql(sock, reader)
    if mode in ("snapshot"):
        test_snapshot(sock, reader)
    if mode in ("types", "all"):
        test_type_casting(sock, reader)
    if mode in ("schema", "all"):
        test_schema_validation_and_sorting(sock, reader)
    if mode in ("aliases", "all"):
        test_sql_aliases(sock, reader)
    if mode in ("case", "all"):
        test_case(sock, reader)
    if mode in ("like", "all"):
        test_like(sock, reader)
    if mode in ("subqueries", "all"):
        test_subqueries(sock, reader)
    if mode in ("union", "all"):
        test_union(sock, reader)
    if mode in ("functions", "all"):
        test_string_functions(sock, reader)
        test_numeric_functions(sock, reader)
        test_datetime_functions(sock, reader)
        test_cast_function(sock, reader)
    
    # New tests
    if mode in ("advanced", "all"):
        test_advanced_commands(sock, reader)
    if mode in ("operators", "all"):
        test_sql_comparison_operators(sock, reader)
    if mode in ("indexing", "all"):
        test_index_maintenance(sock, reader)
    if mode in ("recovery"): # Special case, requires restart
        # This test returns new socket and reader objects
        return test_recovery(sock, reader)
    if mode in ("wrongtype", "all"):
        test_wrongtype_errors(sock, reader)
    
    return sock, reader


if __name__ == "__main__":
    sock = None # define sock in the outer scope
    try:
        sock, reader = create_connection()

        if len(sys.argv) > 1:
            if sys.argv[1] == "bench":
                if len(sys.argv) < 4:
                    print("Usage: python run_tests.py bench COUNT COMMAND [ARGS...]")
                    sys.exit(1)
                count = int(sys.argv[2])
                parts = shlex.split(" ".join(sys.argv[3:]))
                benchmark(sock, reader, parts, count)
            elif sys.argv[1] == "bench-ops":
                if len(sys.argv) < 4:
                    print("Usage: python run_tests.py bench-ops DURATION_SEC COMMAND [ARGS...]")
                    sys.exit(1)
                duration = int(sys.argv[2])
                # Join args and then split to handle quoted arguments correctly
                cmd_line = " ".join(sys.argv[3:])
                parts = shlex.split(cmd_line)
                
                pipeline_size = 50000
                if '--pipeline' in parts:
                    try:
                        idx = parts.index('--pipeline')
                        pipeline_size = int(parts[idx + 1])
                        # remove from parts
                        parts.pop(idx)
                        parts.pop(idx)
                    except (ValueError, IndexError):
                        print("Invalid --pipeline argument. Usage: --pipeline <size>")
                        sys.exit(1)
                benchmark_ops_sec(sock, reader, parts, duration, pipeline_size)
            elif sys.argv[1] == "unit":
                if len(sys.argv) < 3:
                    print("Usage: python test.py unit {json,byte,lists,sets,sql,snapshot,types,schema,aliases,case,like,functions,union,advanced,operators,indexing,recovery,wrongtype,all}")
                    sys.exit(1)
                sock, reader = unit_test(sock, reader, sys.argv[2].lower())

        else:
            print("Enter commands (e.g. SET user:123 '{\"profile\":{\"name\":\"Jane Doe\"}}'):")
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
                    else:
                        parts = shlex.split(line)
                    resp, send, lat, tot = send_resp_command(sock, reader, parts)
                    print(resp, end='')
                    print(f"Send: {send:.2f}ms, Latency: {lat:.2f}ms, Total: {tot:.2f}ms")
            else:
                for line in sys.stdin:
                    line = line.strip()
                    if not line:
                        continue
                    else:
                        parts = shlex.split(line)
                    resp, send, lat, tot = send_resp_command(sock, reader, parts)
                    print(resp, end='')
                    print(f"Send: {send:.2f}ms, Latency: {lat:.2f}ms, Total: {tot:.2f}ms")

    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)
    finally:
        if sock:
            sock.close()




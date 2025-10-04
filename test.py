import sys
import shlex
import argparse

from tests.common import send_resp_command, create_connection, test_result
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
from tests.benchmark import benchmark, benchmark_ops_sec, fill_db
from tests.test_advanced_commands import test_advanced_commands
from tests.test_sql_operators import test_sql_comparison_operators
from tests.test_indexing import test_index_maintenance
from tests.test_recovery import test_recovery
from tests.test_wrongtype_errors import test_wrongtype_errors
from tests.test_data_types_and_constraints import test_data_types_and_constraints
from tests.test_ddl_enhancements import test_ddl_enhancements
from tests.test_dml_enhancements import test_dml_enhancements
from tests.test_dql_enhancements import test_dql_enhancements
from tests.test_cte import test_cte
from tests.test_transactions import test_transactions
from tests.test_vacuum import test_vacuum
from tests.test_graph import test_graph
from tests.test_cypher import test_cypher


# Add prompt_toolkit for better interactive input
try:
    from prompt_toolkit import PromptSession
except ImportError:
    PromptSession = None

def unit_test(conn, reader, mode, ffi_path=None):
    # Existing tests
    if mode in ("byte", "all"):
        test_byte(conn, reader)
    if mode in ("json", "all"):
        test_json(conn, reader)
    if mode in ("lists", "all"):
        test_lists(conn, reader)
    if mode in ("sets", "all"):
        test_sets(conn, reader)
    if mode in ("sql", "all"):
        test_sql(conn, reader)
    if mode == "snapshot":
        test_snapshot(conn, reader)
    if mode in ("types", "all"):
        test_type_casting(conn, reader)
    if mode in ("schema", "all"):
        test_schema_validation_and_sorting(conn, reader)
    if mode in ("aliases", "all"):
        test_sql_aliases(conn, reader)
    if mode in ("case", "all"):
        test_case(conn, reader)
    if mode in ("like", "all"):
        test_like(conn, reader)
    if mode in ("subqueries", "all"):
        test_subqueries(conn, reader)
    if mode in ("union", "all"):
        test_union(conn, reader)
    if mode in ("functions", "all"):
        test_string_functions(conn, reader)
        test_numeric_functions(conn, reader)
        test_datetime_functions(conn, reader)
        test_cast_function(conn, reader)
    
    # New tests
    if mode in ("advanced", "all"):
        test_advanced_commands(conn, reader)
    if mode in ("operators", "all"):
        test_sql_comparison_operators(conn, reader)
    if mode in ("indexing", "all"):
        test_index_maintenance(conn, reader)
    if mode in ("recovery"):
        return test_recovery(conn, reader, ffi_path=ffi_path)
    if mode in ("wrongtype", "all"):
        test_wrongtype_errors(conn, reader)
    if mode in ("constraints", "all"):
        test_data_types_and_constraints(conn, reader)
    if mode in ("ddl_enhancements", "all"):
        test_ddl_enhancements(conn, reader)
    if mode in ("dml_enhancements", "all"):
        test_dml_enhancements(conn, reader)
    if mode in ("dql_enhancements", "all"):
        test_dql_enhancements(conn, reader)
    if mode in ("cte", "all"):
        test_cte(conn, reader)
    if mode in ("transactions", "all"):
        test_transactions(conn, reader, ffi_path=ffi_path)
    if mode in ("vacuum", "all"): # Add this block
        test_vacuum(conn, reader, ffi_path=ffi_path)
    if mode in ("graph", "all"): # Add this block
        test_graph(conn, reader)
    if mode in ("cypher", "all"): # Add this block
        test_cypher(conn, reader)
    
    return conn, reader


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MemFlux Test Runner")
    parser.add_argument('--ffi', dest='ffi_path', help='Path to the dynamic library for FFI testing')
    parser.add_argument('command', nargs='?', help='Command to run (bench, bench-ops, filldb, sql, unit)')
    parser.add_argument('args', nargs=argparse.REMAINDER, help='Arguments for the command')

    parsed_args = parser.parse_args()

    conn = None
    try:
        conn, reader = create_connection(ffi_path=parsed_args.ffi_path)

        if parsed_args.command:
            if parsed_args.command == "bench":
                if len(parsed_args.args) < 2:
                    print("Usage: python test.py bench COUNT COMMAND [ARGS...]")
                    sys.exit(1)
                count = int(parsed_args.args[0])
                parts = shlex.split(" ".join(parsed_args.args[1:]))
                benchmark(conn, reader, parts, count)
            elif parsed_args.command == "bench-ops":
                if len(parsed_args.args) < 3:
                    print("Usage: python test.py bench-ops PIPELINE_SIZE DURATION_SEC COMMAND [ARGS...]")
                    sys.exit(1)
                pipeline_size = int(parsed_args.args[0])
                duration = int(parsed_args.args[1])
                cmd_line = " ".join(parsed_args.args[2:])
                parts = shlex.split(cmd_line)
                
                if '--pipeline' in parts:
                    try:
                        idx = parts.index('--pipeline')
                        pipeline_size = int(parts[idx + 1])
                        parts.pop(idx)
                        parts.pop(idx)
                    except (ValueError, IndexError):
                        print("Invalid --pipeline argument. Usage: --pipeline <size>")
                        sys.exit(1)
                benchmark_ops_sec(conn, reader, parts, duration, pipeline_size)
            elif parsed_args.command == "filldb":
                if len(parsed_args.args) < 1:
                    print("Usage: python test.py filldb <MBs>")
                    sys.exit(1)
                try:
                    mbs = int(parsed_args.args[0])
                except ValueError:
                    print("Error: MBs must be an integer.")
                    sys.exit(1)
                fill_db(conn, reader, mbs)
            elif parsed_args.command == "sql":
                print("Entering SQL mode. Type your SQL commands.")
                if PromptSession is not None:
                    session = PromptSession()
                    while True:
                        try:
                            line = session.prompt("sql> ")
                        except (EOFError, KeyboardInterrupt):
                            print()
                            break
                        line = line.strip()
                        if not line:
                            continue
                        else:
                            parts = ['SQL', line]
                        resp, send, lat, tot = send_resp_command(conn, reader, parts)
                        print(resp, end='')
                        print(f"Send: {send:.2f}ms, Latency: {lat:.2f}ms, Total: {tot:.2f}ms")
                else:
                    for line in sys.stdin:
                        line = line.strip()
                        if not line:
                            continue
                        else:
                            parts = ['SQL', line]
                        resp, send, lat, tot = send_resp_command(conn, reader, parts)
                        print(resp, end='')
                        print(f"Send: {send:.2f}ms, Latency: {lat:.2f}ms, Total: {tot:.2f}ms")
            elif parsed_args.command == "unit":
                if not parsed_args.args:
                    print("Usage: python test.py unit {json,byte,lists,sets,sql,snapshot,types,schema,aliases,case,like,functions,union,advanced,operators,indexing,recovery,wrongtype,constraints,ddl_enhancements,dml_enhancements,dql_enhancements,cte,transactions,vacuum,graph,cypher,all}")
                    sys.exit(1)
                mode = parsed_args.args[0]
                conn, reader = unit_test(conn, reader, mode.lower(), ffi_path=parsed_args.ffi_path)
                test_result.summary()

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
                    if line.upper().startswith('SQL'):
                        parts = ['SQL', line[3:].lstrip()]
                    else:
                        parts = shlex.split(line)
                    resp, send, lat, tot = send_resp_command(conn, reader, parts)
                    print(resp, end='')
                    print(f"Send: {send:.2f}ms, Latency: {lat:.2f}ms, Total: {tot:.2f}ms")
            else:
                for line in sys.stdin:
                    line = line.strip()
                    if not line:
                        continue
                    if line.upper().startswith('SQL'):
                        parts = ['SQL', line[3:].lstrip()]
                    else:
                        parts = shlex.split(line)
                    resp, send, lat, tot = send_resp_command(conn, reader, parts)
                    print(resp, end='')
                    print(f"Send: {send:.2f}ms, Latency: {lat:.2f}ms, Total: {tot:.2f}ms")

    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()

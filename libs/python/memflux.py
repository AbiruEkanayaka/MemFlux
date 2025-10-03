# memflux.py

import ctypes
import json
import shlex
import sys
import threading
from enum import IntEnum
from typing import Any, Dict, List, Optional, Sequence, Union, Iterator, Tuple

class MemfluxError(Exception):
    """Base exception for all memflux-related errors."""
    pass

class InterfaceError(MemfluxError):
    """Exception for errors related to the database interface rather than the database itself."""
    pass

class DatabaseError(MemfluxError):
    """Exception for errors related to the database."""
    pass


class _FFIResponseType(IntEnum):
    """Maps to the FFIResponseType enum in Rust."""
    OK = 0
    SIMPLE_STRING = 1
    BYTES = 2
    MULTI_BYTES = 3
    INTEGER = 4
    NIL = 5
    ERROR = 6

class _FFIResponse(ctypes.Structure):
    """Maps to the FFIResponse struct in Rust."""
    _fields_ = [
        ("response_type", ctypes.c_int),
        ("string_value", ctypes.c_char_p),
        ("bytes_value", ctypes.c_void_p),
        ("bytes_len", ctypes.c_size_t),
        ("multi_bytes_value", ctypes.POINTER(ctypes.c_void_p)),
        ("multi_bytes_lens", ctypes.POINTER(ctypes.c_size_t)),
        ("multi_bytes_count", ctypes.c_size_t),
        ("integer_value", ctypes.c_int64),
    ]

# Define a type hint for the opaque database handle
_DB_HANDLE = ctypes.c_void_p
_CURSOR_HANDLE = ctypes.c_void_p

# --- Main Wrapper Classes ---

class Cursor:
    """
    A cursor object to interact with the MemFlux database.

    It allows executing commands and fetching results. Cursors should be
    created by calling Connection.cursor(). Each cursor has its own
    transactional context.
    """
    def __init__(self, connection: 'Connection', cursor_handle: _CURSOR_HANDLE):
        if not cursor_handle:
            raise InterfaceError("Failed to initialize cursor: received null handle.")
        self.connection = connection
        self._handle = cursor_handle
        self._is_closed = False
        self._last_result: Optional[List[Any]] = None
        self._row_iterator: Optional[Iterator[Any]] = None
        self._last_result_type: Optional[_FFIResponseType] = None

    def __enter__(self) -> 'Cursor':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __iter__(self) -> 'Cursor':
        if self._row_iterator is None:
            # If execute() hasn't been called, this will be an empty list.
            self._row_iterator = iter(self._last_result or [])
        return self

    def __next__(self) -> Any:
        if self._row_iterator is None:
            self.__iter__() # Initialize the iterator
        try:
            # Mypy struggles with __next__ on Optional[Iterator]
            return next(self._row_iterator) # type: ignore
        except StopIteration:
            # Reset for next potential iteration
            self._row_iterator = None
            raise

    def close(self) -> None:
        """Closes the cursor, releasing its handle in the Rust library."""
        if not self._is_closed:
            self.connection._lib.memflux_cursor_close(self._handle)
            self._handle = None
            self._is_closed = True
            self._last_result = None
            self._row_iterator = None

    @property
    def rowcount(self) -> int:
        """Returns the number of rows from the last SELECT/RETURNING query."""
        if self._last_result is None:
            return -1
        return len(self._last_result)

    def execute(self, command: str, params: Optional[Sequence[Any]] = None) -> 'Cursor':
        """
        Executes a database command.

        Args:
            command: The command string to execute (e.g., "SET key value", "SQL ...").
            params: A sequence of parameters to bind to '?' placeholders in the command.

        Returns:
            The cursor instance.
        """
        if self.connection.is_closed:
            raise InterfaceError("Cannot operate on a closed connection.")
        if self._is_closed:
            raise InterfaceError("Cannot operate on a closed cursor.")

        self._last_result = None
        self._row_iterator = None
        self._last_result_type = None

        final_command = self._bind_params(command, params)

        command_parts = final_command.lstrip().split(maxsplit=1)
        if command_parts and command_parts[0].upper() == 'SQL':
            if len(command_parts) > 1:
                final_command = f"SQL {shlex.quote(command_parts[1])}"
            else:
                final_command = "SQL"

        response_ptr = None
        try:
            command_c_str = final_command.encode('utf-8')
            
            response_ptr = self.connection._lib.memflux_exec(
                self.connection._shared_handle, 
                self._handle, 
                command_c_str
            )
            if not response_ptr:
                raise DatabaseError("FFI call to 'memflux_exec' returned a null pointer.")
            
            response_type, result_list = self._process_response(response_ptr.contents)
            self._last_result_type = response_type
            self._last_result = result_list

        finally:
            if response_ptr:
                self.connection._lib.memflux_response_free(response_ptr)
        
        return self

    def fetchone(self) -> Optional[Any]:
        """Fetches the next row of a query result set."""
        try:
            return self.__next__()
        except StopIteration:
            return None

    def fetchall(self) -> List[Any]:
        """Fetches all remaining rows of a query result set."""
        if self._row_iterator is None:
            self.__iter__()
        
        remaining_rows = list(self._row_iterator) # type: ignore
        self._row_iterator = None # Exhaust the iterator
        return remaining_rows

    def fetchmany(self, size: int = 1) -> List[Any]:
        """Fetches the next set of rows of a query result, returning a list."""
        if size < 0:
            return self.fetchall()
            
        if self._row_iterator is None:
            self.__iter__()

        batch = []
        for _ in range(size):
            try:
                batch.append(next(self._row_iterator)) # type: ignore
            except StopIteration:
                self._row_iterator = None
                break
        return batch

    def _bind_params(self, command: str, params: Optional[Sequence[Any]]) -> str:
        """Replaces '?' placeholders with properly quoted parameters."""
        if not params:
            return command
            
        param_iter = iter(params)
        parts = command.split('?')
        if len(parts) - 1 != len(params):
            raise InterfaceError(
                f"Incorrect number of bindings supplied. The command has {len(parts) - 1} "
                f"placeholders, but {len(params)} parameters were provided."
            )

        final_parts = [parts[0]]
        for i in range(1, len(parts)):
            try:
                param = next(param_iter)
                if isinstance(param, str):
                    quoted_param = f"'{param.replace("'", "''")}'"
                elif isinstance(param, (int, float)):
                    quoted_param = str(param)
                elif param is None:
                    quoted_param = "NULL"
                elif isinstance(param, bytes):
                    try:
                        decoded_param = param.decode('utf-8')
                        quoted_param = f"'{decoded_param.replace("'", "''")}'"
                    except UnicodeDecodeError:
                        hex_param = param.hex()
                        quoted_param = f"'\\x{hex_param}'"
                else:
                    quoted_param = shlex.quote(str(param))
                
                final_parts.append(quoted_param)
                final_parts.append(parts[i])
            except StopIteration:
                raise InterfaceError("More placeholders than parameters.")

        return "".join(final_parts)

    def _process_response(self, resp: _FFIResponse) -> Tuple[_FFIResponseType, List[Any]]:
        """Converts an FFIResponse struct into Python objects."""
        response_type = _FFIResponseType(resp.response_type)

        if response_type == _FFIResponseType.ERROR:
            err_msg = resp.string_value.decode('utf-8') if resp.string_value else "Unknown FFI error"
            raise DatabaseError(err_msg)

        if response_type == _FFIResponseType.OK:
            return (response_type, [])
            
        if response_type == _FFIResponseType.SIMPLE_STRING:
            val = resp.string_value.decode('utf-8') if resp.string_value else ""
            return (response_type, [val])

        if response_type == _FFIResponseType.BYTES:
            if not resp.bytes_value:
                return (response_type, [b''])
            val_bytes = ctypes.string_at(resp.bytes_value, resp.bytes_len)
            try:
                return (response_type, [json.loads(val_bytes.decode('utf-8'))])
            except (json.JSONDecodeError, UnicodeDecodeError):
                return (response_type, [val_bytes])
                
        if response_type == _FFIResponseType.MULTI_BYTES:
            if not resp.multi_bytes_value or not resp.multi_bytes_lens:
                return (response_type, [])
            results = []
            for i in range(resp.multi_bytes_count):
                if not resp.multi_bytes_value[i]:
                    results.append(b'')
                    continue
                item_bytes = ctypes.string_at(resp.multi_bytes_value[i], resp.multi_bytes_lens[i])
                try:
                    results.append(json.loads(item_bytes.decode('utf-8')))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    results.append(item_bytes)
            return (response_type, results)

        if response_type == _FFIResponseType.INTEGER:
            return (response_type, [resp.integer_value])

        if response_type == _FFIResponseType.NIL:
            return (response_type, [])

        raise InterfaceError(f"Received unknown response type from FFI: {response_type}")


class Connection:
    """
    A connection to a MemFlux database.

    Connections should be created using the top-level memflux.connect() function.
    """
    def __init__(self, db_handle: _DB_HANDLE, library: ctypes.CDLL):
        if not db_handle:
            raise InterfaceError("Failed to initialize database: received null handle.")
        self._shared_handle = db_handle
        self._lib = library
        self.is_closed = False
        self._lock = threading.Lock()

    def __enter__(self) -> 'Connection':
        if self.is_closed:
             raise InterfaceError("Cannot operate on a closed connection.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def close(self) -> None:
        """Closes the database connection."""
        with self._lock:
            if not self.is_closed:
                self._lib.memflux_close(self._shared_handle)
                self._shared_handle = None
                self.is_closed = True

    def cursor(self) -> Cursor:
        """Creates a new cursor to perform database operations."""
        with self._lock:
            if self.is_closed:
                raise InterfaceError("Cannot operate on a closed connection.")
            cursor_handle = self._lib.memflux_cursor_open(self._shared_handle)
            if not cursor_handle:
                raise InterfaceError("Failed to create a new cursor from the FFI.")
        return Cursor(self, cursor_handle)


def connect(config: Dict[str, Any], lib: str) -> Connection:
    """
    Establishes a connection to the MemFlux database.

    Args:
        config: A dictionary with configuration parameters for the database instance.
        lib: The file path to the `libmemflux` dynamic library (.so, .dll, .dylib).

    Returns:
        A Connection object.
    """
    try:
        library = ctypes.CDLL(lib)
    except OSError as e:
        raise InterfaceError(f"Failed to load the dynamic library at '{lib}'. Please ensure the path is correct and the library is compatible with your system.") from e

    # --- Define FFI function signatures ---
    try:
        library.memflux_open.argtypes = [ctypes.c_char_p]
        library.memflux_open.restype = _DB_HANDLE
        
        library.memflux_close.argtypes = [_DB_HANDLE]
        library.memflux_close.restype = None

        library.memflux_cursor_open.argtypes = [_DB_HANDLE]
        library.memflux_cursor_open.restype = _CURSOR_HANDLE

        library.memflux_cursor_close.argtypes = [_CURSOR_HANDLE]
        library.memflux_cursor_close.restype = None

        library.memflux_exec.argtypes = [_DB_HANDLE, _CURSOR_HANDLE, ctypes.c_char_p]
        library.memflux_exec.restype = ctypes.POINTER(_FFIResponse)

        library.memflux_response_free.argtypes = [ctypes.POINTER(_FFIResponse)]
        library.memflux_response_free.restype = None
    except AttributeError as e:
        raise InterfaceError(f"The library '{lib}' is missing one or more required FFI functions. Please ensure it was compiled correctly. Missing: {e}")


    # --- Open the database ---
    config_json_str = json.dumps(config)
    config_c_str = config_json_str.encode('utf-8')
    
    db_handle = library.memflux_open(config_c_str)

    if not db_handle:
        raise DatabaseError("Failed to open MemFlux database. Check configuration and file permissions.")

    return Connection(db_handle, library)

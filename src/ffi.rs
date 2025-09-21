use crate::config::{Config, FFIConfig};
use crate::types::{Command, Response};
use crate::MemFluxDB;
use libc::{c_char, size_t};
use once_cell::sync::Lazy;
use std::ffi::{CStr, CString};
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::RwLock;

// A global Tokio runtime for the FFI layer.
static TOKIO_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime for FFI")
});

// The opaque pointer to the shared database instance.
pub struct MemFluxDBSharedHandle(Arc<MemFluxDB>);

// The opaque pointer to a cursor, which holds the transaction state.
pub struct MemFluxCursorHandle(crate::transaction::TransactionHandle);

// The C-compatible response structure.
#[repr(C)]
pub enum FFIResponseType {
    Ok,
    SimpleString,
    Bytes,
    MultiBytes,
    Integer,
    Nil,
    Error,
}

#[repr(C)]
pub struct FFIResponse {
    pub response_type: FFIResponseType,
    pub string_value: *mut c_char,
    pub bytes_value: *mut c_char,
    pub bytes_len: size_t,
    pub multi_bytes_value: *mut *mut c_char,
    pub multi_bytes_lens: *mut size_t,
    pub multi_bytes_count: size_t,
    pub integer_value: i64,
}

#[unsafe(no_mangle)]
pub extern "C" fn memflux_open(config_json: *const c_char) -> *mut MemFluxDBSharedHandle {
    let config_str = match unsafe { CStr::from_ptr(config_json).to_str() } {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };

    let ffi_config: FFIConfig = match serde_json::from_str(config_str) {
        Ok(c) => c,
        Err(_) => return std::ptr::null_mut(),
    };

    let config: Config = ffi_config.into();

    let db = match TOKIO_RUNTIME.block_on(MemFluxDB::open_with_config(config)) {
        Ok(db) => db,
        Err(_) => return std::ptr::null_mut(),
    };

    let handle = Box::new(MemFluxDBSharedHandle(Arc::new(db)));
    Box::into_raw(handle)
}

#[unsafe(no_mangle)]
pub extern "C" fn memflux_close(handle: *mut MemFluxDBSharedHandle) {
    if !handle.is_null() {
        unsafe {
            drop(Box::from_raw(handle));
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn memflux_cursor_open(handle: *mut MemFluxDBSharedHandle) -> *mut MemFluxCursorHandle {
    if handle.is_null() {
        return std::ptr::null_mut();
    }
    let transaction_handle = Arc::new(RwLock::new(None));
    let cursor_handle = Box::new(MemFluxCursorHandle(transaction_handle));
    Box::into_raw(cursor_handle)
}

#[unsafe(no_mangle)]
pub extern "C" fn memflux_cursor_close(handle: *mut MemFluxCursorHandle) {
    if !handle.is_null() {
        unsafe {
            drop(Box::from_raw(handle));
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn memflux_exec(
    db_handle: *mut MemFluxDBSharedHandle,
    cursor_handle: *mut MemFluxCursorHandle,
    command_str: *const c_char,
) -> *mut FFIResponse {
    let shared_handle = match unsafe { db_handle.as_ref() } {
        Some(h) => h,
        None => return std::ptr::null_mut(),
    };
    let cursor_handle = match unsafe { cursor_handle.as_ref() } {
        Some(h) => h,
        None => return std::ptr::null_mut(),
    };

    let db = &shared_handle.0;
    let transaction_handle = cursor_handle.0.clone();

    let command_c_str = unsafe { CStr::from_ptr(command_str) };

    let command_str_result = command_c_str.to_str();
    let command_str = match command_str_result {
        Ok(s) => s,
        Err(_) => {
            let err_resp = Response::Error("Invalid UTF-8 in command string".to_string());
            return Box::into_raw(Box::new(response_to_ffi(err_resp)));
        }
    };
    
    let command_parts: Vec<String> = match shlex::split(command_str) {
        Some(parts) => parts,
        None => {
            let err_resp = Response::Error("Invalid command string".to_string());
            return Box::into_raw(Box::new(response_to_ffi(err_resp)));
        }
    };

    if command_parts.is_empty() {
        let err_resp = Response::Error("Empty command string".to_string());
        return Box::into_raw(Box::new(response_to_ffi(err_resp)));
    }

    let command_name = command_parts[0].to_uppercase();
    let args: Vec<Vec<u8>> = command_parts.iter().map(|s| s.as_bytes().to_vec()).collect();

    let command = Command { name: command_name, args };


    let response = TOKIO_RUNTIME.block_on(db.execute_command(command, transaction_handle));

    Box::into_raw(Box::new(response_to_ffi(response)))
}

#[unsafe(no_mangle)]
pub extern "C" fn memflux_response_free(response: *mut FFIResponse) {
    if response.is_null() {
        return;
    }
    unsafe {
        let resp = Box::from_raw(response);
        match resp.response_type {
            FFIResponseType::SimpleString | FFIResponseType::Error => {
                if !resp.string_value.is_null() {
                    drop(CString::from_raw(resp.string_value));
                }
            }
            FFIResponseType::Bytes => {
                if !resp.bytes_value.is_null() {
                    drop(Vec::from_raw_parts(
                        resp.bytes_value as *mut u8,
                        resp.bytes_len,
                        resp.bytes_len,
                    ));
                }
            }
            FFIResponseType::MultiBytes => {
                if resp.multi_bytes_count > 0 && !resp.multi_bytes_value.is_null() {
                    let ptrs = Vec::from_raw_parts(
                        resp.multi_bytes_value,
                        resp.multi_bytes_count,
                        resp.multi_bytes_count,
                    );
                    let lens = Vec::from_raw_parts(
                        resp.multi_bytes_lens,
                        resp.multi_bytes_count,
                        resp.multi_bytes_count,
                    );
                    for i in 0..resp.multi_bytes_count {
                        // An empty vector can still have a non-null pointer, so we must always
                        // reconstruct and drop it to avoid leaking its (potentially empty) allocation.
                        drop(Vec::from_raw_parts(ptrs[i] as *mut u8, lens[i], lens[i]));
                    }
                }
            }
            _ => {}
        }
    }
}

fn response_to_ffi(response: Response) -> FFIResponse {
    let mut ffi_resp = FFIResponse {
        response_type: FFIResponseType::Nil,
        string_value: std::ptr::null_mut(),
        bytes_value: std::ptr::null_mut(),
        bytes_len: 0,
        multi_bytes_value: std::ptr::null_mut(),
        multi_bytes_lens: std::ptr::null_mut(),
        multi_bytes_count: 0,
        integer_value: 0,
    };

    match response {
        Response::Ok => {
            ffi_resp.response_type = FFIResponseType::SimpleString;
            ffi_resp.string_value = CString::new("OK").unwrap().into_raw();
        }
        Response::SimpleString(s) => {
            ffi_resp.response_type = FFIResponseType::SimpleString;
            ffi_resp.string_value = CString::new(s).unwrap().into_raw();
        }
        Response::Error(s) => {
            ffi_resp.response_type = FFIResponseType::Error;
            ffi_resp.string_value = CString::new(s).unwrap().into_raw();
        }
        Response::Bytes(mut b) => {
            ffi_resp.response_type = FFIResponseType::Bytes;
            b.shrink_to_fit();
            ffi_resp.bytes_len = b.len();
            if b.is_empty() {
                ffi_resp.bytes_value = std::ptr::null_mut();
            } else {
                ffi_resp.bytes_value = b.as_mut_ptr() as *mut c_char;
                std::mem::forget(b);
            }
        }
        Response::MultiBytes(vals) => {
            ffi_resp.response_type = FFIResponseType::MultiBytes;
            ffi_resp.multi_bytes_count = vals.len();
            if vals.is_empty() {
                ffi_resp.multi_bytes_value = std::ptr::null_mut();
                ffi_resp.multi_bytes_lens = std::ptr::null_mut();
            } else {
                let mut ptrs: Vec<*mut c_char> = Vec::with_capacity(vals.len());
                let mut lens: Vec<size_t> = Vec::with_capacity(vals.len());

                for mut v in vals {
                    v.shrink_to_fit();
                    lens.push(v.len());
                    ptrs.push(v.as_mut_ptr() as *mut c_char);
                    std::mem::forget(v);
                }

                ptrs.shrink_to_fit();
                ffi_resp.multi_bytes_value = ptrs.as_mut_ptr();
                std::mem::forget(ptrs);

                lens.shrink_to_fit();
                ffi_resp.multi_bytes_lens = lens.as_mut_ptr();
                std::mem::forget(lens);
            }
        }
        Response::Integer(i) => {
            ffi_resp.response_type = FFIResponseType::Integer;
            ffi_resp.integer_value = i;
        }
        Response::Nil => {
            ffi_resp.response_type = FFIResponseType::Nil;
        }
    }

    ffi_resp
}
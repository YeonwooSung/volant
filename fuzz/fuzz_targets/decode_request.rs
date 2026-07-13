//! Fuzz target: request/response payload decode must never panic.
#![no_main]

use libfuzzer_sys::fuzz_target;
use volant_protocol::{decode_request, decode_response};

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    let opcode = u16::from_le_bytes([data[0], data.get(1).copied().unwrap_or(0)]);
    let payload = if data.len() > 2 { &data[2..] } else { &[] };
    let _ = decode_request(opcode, payload);
    let _ = decode_response(opcode, payload);
});

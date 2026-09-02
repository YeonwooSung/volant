//! Fuzz target: membership (100–107) + txn (32/50/52) request/response decode.
//!
//! More interesting than another frame decoder: focused opcodes plus the
//! same raw LE-opcode path as `decode_request`.
#![no_main]

use libfuzzer_sys::fuzz_target;
use volant_protocol::{decode_request, decode_response};

/// InitProducerId / BeginTxn / EndTxn + v0.10 membership req/resp opcodes.
const FOCUS: &[u16] = &[32, 50, 52, 100, 101, 102, 103, 104, 105, 106, 107];

fuzz_target!(|data: &[u8]| {
    if !data.is_empty() {
        let op = FOCUS[(data[0] as usize) % FOCUS.len()];
        let payload = &data[1..];
        let _ = decode_request(op, payload);
        let _ = decode_response(op, payload);
    }
    // Same entry as decode_request: opcode LE + payload (unknown / adjacent).
    if data.len() >= 2 {
        let opcode = u16::from_le_bytes([data[0], data[1]]);
        let payload = &data[2..];
        let _ = decode_request(opcode, payload);
        let _ = decode_response(opcode, payload);
    } else if data.len() == 1 {
        let opcode = u16::from_le_bytes([data[0], 0]);
        let _ = decode_request(opcode, &[]);
        let _ = decode_response(opcode, &[]);
    }
});

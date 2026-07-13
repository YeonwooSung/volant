//! Fuzz target: frame decode must never panic.
#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use volant_protocol::codec::decode_frame;

fuzz_target!(|data: &[u8]| {
    let mut buf = BytesMut::from(data);
    let _ = decode_frame(&mut buf);
    // Second pass after partial consume (if any).
    let _ = decode_frame(&mut buf);
});

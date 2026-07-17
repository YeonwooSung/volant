//! Shared classic / flexible Kafka wire helpers for request parsing.
//!
//! Keeps handler code free of repeated `if flex { compact } else { classic }` branches.

use bytes::Buf;
use volant_core::{Error, Result};

use super::codec::{
    get_compact_array_len, get_compact_nullable_string, get_compact_string, get_nullable_string,
    get_string,
};

/// Read a non-null string (classic STRING or flexible COMPACT_STRING).
pub fn read_string(src: &mut impl Buf, flex: bool) -> Result<String> {
    if flex {
        get_compact_string(src)
    } else {
        get_string(src)
    }
}

/// Read a nullable string (classic NULLABLE_STRING or flexible COMPACT_NULLABLE_STRING).
pub fn read_nullable_string(src: &mut impl Buf, flex: bool) -> Result<Option<String>> {
    if flex {
        get_compact_nullable_string(src)
    } else {
        get_nullable_string(src)
    }
}

/// Read an array length.
///
/// Flexible: compact array length (`None` = null array, `Some(0)` = empty).
/// Classic: INT32 count (`None` never returned; negative treated as empty `Some(0)`).
pub fn read_array_len(src: &mut impl Buf, flex: bool) -> Result<Option<usize>> {
    if flex {
        get_compact_array_len(src)
    } else {
        if src.remaining() < 4 {
            return Err(Error::Protocol("truncated classic array length".into()));
        }
        let n = src.get_i32();
        Ok(Some(n.max(0) as usize))
    }
}

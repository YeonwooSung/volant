//! Kafka RecordBatch compression codecs (Phase 28).

use std::io::{Read, Write};

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use volant_core::{Error, Result};

/// Compression codec in RecordBatch attributes bits 0–2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionCodec {
    /// No compression.
    None = 0,
    /// GZIP.
    Gzip = 1,
    /// Snappy (Xerial framed preferred).
    Snappy = 2,
    /// LZ4.
    Lz4 = 3,
    /// Zstd.
    Zstd = 4,
}

impl CompressionCodec {
    /// Parse from attributes low 3 bits.
    pub fn from_attributes(attributes: i16) -> Result<Self> {
        match attributes & 0x07 {
            0 => Ok(Self::None),
            1 => Ok(Self::Gzip),
            2 => Ok(Self::Snappy),
            3 => Ok(Self::Lz4),
            4 => Ok(Self::Zstd),
            other => Err(Error::Protocol(format!(
                "unsupported record batch compression codec {other}"
            ))),
        }
    }

    /// Attributes low bits value.
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Decompress RecordBatch records payload.
pub fn decompress(codec: CompressionCodec, data: &[u8]) -> Result<Vec<u8>> {
    match codec {
        CompressionCodec::None => Ok(data.to_vec()),
        CompressionCodec::Gzip => gzip_decompress(data),
        CompressionCodec::Snappy => snappy_decompress(data),
        CompressionCodec::Lz4 => lz4_decompress(data),
        CompressionCodec::Zstd => zstd_decompress(data),
    }
}

/// Compress RecordBatch records payload.
pub fn compress(codec: CompressionCodec, data: &[u8]) -> Result<Vec<u8>> {
    match codec {
        CompressionCodec::None => Ok(data.to_vec()),
        CompressionCodec::Gzip => gzip_compress(data),
        CompressionCodec::Snappy => snappy_compress(data),
        CompressionCodec::Lz4 => lz4_compress(data),
        CompressionCodec::Zstd => zstd_compress(data),
    }
}

fn gzip_decompress(data: &[u8]) -> Result<Vec<u8>> {
    let mut dec = GzDecoder::new(data);
    let mut out = Vec::new();
    dec.read_to_end(&mut out)
        .map_err(|e| Error::Protocol(format!("gzip decompress: {e}")))?;
    Ok(out)
}

fn gzip_compress(data: &[u8]) -> Result<Vec<u8>> {
    let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
    enc.write_all(data)
        .map_err(|e| Error::Protocol(format!("gzip compress: {e}")))?;
    enc.finish()
        .map_err(|e| Error::Protocol(format!("gzip finish: {e}")))
}

/// Xerial snappy stream header used by Kafka (`SnappyOutputStream`).
const XERIAL_MAGIC: &[u8] = &[0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0];

fn snappy_decompress(data: &[u8]) -> Result<Vec<u8>> {
    if data.starts_with(XERIAL_MAGIC) {
        return snappy_decompress_xerial(data);
    }
    // Raw snappy block fallback.
    let mut dec = snap::raw::Decoder::new();
    dec.decompress_vec(data)
        .map_err(|e| Error::Protocol(format!("snappy raw decompress: {e}")))
}

fn snappy_decompress_xerial(data: &[u8]) -> Result<Vec<u8>> {
    // magic(8) + version(4 BE) + compatible(4 BE) + chunks of [len BE][snappy block]
    if data.len() < 16 {
        return Err(Error::Protocol("truncated xerial snappy header".into()));
    }
    let mut off = 16;
    let mut out = Vec::new();
    let mut dec = snap::raw::Decoder::new();
    while off + 4 <= data.len() {
        let comp_len = u32::from_be_bytes(data[off..off + 4].try_into().unwrap()) as usize;
        off += 4;
        if off + comp_len > data.len() {
            return Err(Error::Protocol("truncated xerial snappy block".into()));
        }
        let block = &data[off..off + comp_len];
        off += comp_len;
        let piece = dec
            .decompress_vec(block)
            .map_err(|e| Error::Protocol(format!("snappy xerial block: {e}")))?;
        out.extend_from_slice(&piece);
    }
    Ok(out)
}

fn snappy_compress(data: &[u8]) -> Result<Vec<u8>> {
    // Emit Xerial framed stream (Kafka-compatible).
    let mut enc = snap::raw::Encoder::new();
    let compressed = enc
        .compress_vec(data)
        .map_err(|e| Error::Protocol(format!("snappy compress: {e}")))?;
    let mut out = Vec::with_capacity(16 + 4 + compressed.len());
    out.extend_from_slice(XERIAL_MAGIC);
    out.extend_from_slice(&1u32.to_be_bytes()); // version
    out.extend_from_slice(&1u32.to_be_bytes()); // compatible
    out.extend_from_slice(&(compressed.len() as u32).to_be_bytes());
    out.extend_from_slice(&compressed);
    Ok(out)
}

fn lz4_decompress(data: &[u8]) -> Result<Vec<u8>> {
    // Prefer LZ4 frame (common for modern Kafka clients).
    {
        let mut dec = lz4_flex::frame::FrameDecoder::new(data);
        let mut out = Vec::new();
        if dec.read_to_end(&mut out).is_ok() {
            return Ok(out);
        }
    }
    // Some producers emit a single LZ4 block without frame.
    if let Ok(out) = lz4_flex::block::decompress_size_prepended(data) {
        return Ok(out);
    }
    // Last resort: try increasing output sizes.
    for mult in [4usize, 8, 16, 32, 64] {
        let max = data.len().saturating_mul(mult).max(1024);
        if let Ok(out) = lz4_flex::block::decompress(data, max) {
            return Ok(out);
        }
    }
    Err(Error::Protocol("lz4 decompress failed".into()))
}

fn lz4_compress(data: &[u8]) -> Result<Vec<u8>> {
    // Frame format is widely accepted by Kafka consumers.
    let mut out = Vec::new();
    {
        let mut enc = lz4_flex::frame::FrameEncoder::new(&mut out);
        enc.write_all(data)
            .map_err(|e| Error::Protocol(format!("lz4 compress: {e}")))?;
        enc.finish()
            .map_err(|e| Error::Protocol(format!("lz4 finish: {e}")))?;
    }
    Ok(out)
}

fn zstd_decompress(data: &[u8]) -> Result<Vec<u8>> {
    zstd::stream::decode_all(data).map_err(|e| Error::Protocol(format!("zstd decompress: {e}")))
}

fn zstd_compress(data: &[u8]) -> Result<Vec<u8>> {
    zstd::stream::encode_all(data, 1).map_err(|e| Error::Protocol(format!("zstd compress: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gzip_roundtrip() {
        let src = b"hello gzip kafka records payload";
        let c = compress(CompressionCodec::Gzip, src).unwrap();
        assert_ne!(c, src);
        let d = decompress(CompressionCodec::Gzip, &c).unwrap();
        assert_eq!(d, src);
    }

    #[test]
    fn snappy_xerial_roundtrip() {
        let src = b"snappy xerial framed kafka payload 0123456789";
        let c = compress(CompressionCodec::Snappy, src).unwrap();
        assert!(c.starts_with(XERIAL_MAGIC));
        let d = decompress(CompressionCodec::Snappy, &c).unwrap();
        assert_eq!(d, src);
    }

    #[test]
    fn lz4_roundtrip() {
        let src = b"lz4 frame kafka payload ".repeat(20);
        let c = compress(CompressionCodec::Lz4, &src).unwrap();
        let d = decompress(CompressionCodec::Lz4, &c).unwrap();
        assert_eq!(d, src);
    }

    #[test]
    fn zstd_roundtrip() {
        let src = b"zstd kafka payload ".repeat(30);
        let c = compress(CompressionCodec::Zstd, &src).unwrap();
        let d = decompress(CompressionCodec::Zstd, &c).unwrap();
        assert_eq!(d, src);
    }
}

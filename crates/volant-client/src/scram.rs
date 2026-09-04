//! Client-side SCRAM proof computation (Phase 22 SHA-256; v0.238 SHA-512).
//!
//! Matches broker AuthMessage construction (no channel binding).

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use rand::RngCore;
use sha2::{Digest, Sha256, Sha512};
use volant_core::{Error, Result};

type HmacSha256 = Hmac<Sha256>;
type HmacSha512 = Hmac<Sha512>;

/// Generate a printable client nonce.
pub fn generate_client_nonce() -> String {
    let mut buf = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut buf);
    B64.encode(buf).replace(',', "A")
}

/// Compute client proof and expected server signature for SCRAM-SHA-256.
pub fn client_proof_and_server_sig(
    username: &str,
    password: &str,
    client_nonce: &str,
    combined_nonce: &str,
    salt: &[u8],
    iterations: u32,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut salted = [0u8; 32];
    pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut salted);
    let client_key = hmac_sha256(&salted, b"Client Key")?;
    let stored_key = sha256(&client_key);
    let server_key = hmac_sha256(&salted, b"Server Key")?;
    let auth_message = build_auth_message(username, client_nonce, combined_nonce, salt, iterations);
    let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes())?;
    let mut proof = vec![0u8; 32];
    for i in 0..32 {
        proof[i] = client_key[i] ^ client_signature[i];
    }
    let server_sig = hmac_sha256(&server_key, auth_message.as_bytes())?;
    Ok((proof, server_sig))
}

/// Compute client proof and expected server signature for SCRAM-SHA-512 (v0.238).
pub fn client_proof_and_server_sig_sha512(
    username: &str,
    password: &str,
    client_nonce: &str,
    combined_nonce: &str,
    salt: &[u8],
    iterations: u32,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut salted = [0u8; 64];
    pbkdf2_hmac::<Sha512>(password.as_bytes(), salt, iterations, &mut salted);
    let client_key = hmac_sha512(&salted, b"Client Key")?;
    let stored_key = sha512(&client_key);
    let server_key = hmac_sha512(&salted, b"Server Key")?;
    let auth_message = build_auth_message(username, client_nonce, combined_nonce, salt, iterations);
    let client_signature = hmac_sha512(&stored_key, auth_message.as_bytes())?;
    let mut proof = vec![0u8; 64];
    for i in 0..64 {
        proof[i] = client_key[i] ^ client_signature[i];
    }
    let server_sig = hmac_sha512(&server_key, auth_message.as_bytes())?;
    Ok((proof, server_sig))
}

fn sha256(data: &[u8]) -> Vec<u8> {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().to_vec()
}

fn sha512(data: &[u8]) -> Vec<u8> {
    let mut h = Sha512::new();
    h.update(data);
    h.finalize().to_vec()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|e| Error::InvalidArgument(format!("hmac key: {e}")))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hmac_sha512(key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
    let mut mac = HmacSha512::new_from_slice(key)
        .map_err(|e| Error::InvalidArgument(format!("hmac key: {e}")))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn build_auth_message(
    username: &str,
    client_nonce: &str,
    combined_nonce: &str,
    salt: &[u8],
    iterations: u32,
) -> String {
    let client_first_bare = format!("n={username},r={client_nonce}");
    let server_first = format!("r={combined_nonce},s={},i={iterations}", B64.encode(salt));
    let client_final_wo_proof = format!("c=biws,r={combined_nonce}");
    format!("{client_first_bare},{server_first},{client_final_wo_proof}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex_of(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    #[test]
    fn sha256_pinned_vector() {
        let (proof, sig) = client_proof_and_server_sig(
            "alice",
            "s3cret",
            "rOprNGfwEbeRWgbNEkqO",
            "rOprNGfwEbeRWgbNEkqOserver",
            b"saltSALTsaltSALT",
            4096,
        )
        .unwrap();
        assert_eq!(proof.len(), 32);
        assert_eq!(
            hex_of(&proof),
            "82aa6ee69043dd3c43785fba02fe220ea4a74a44b12d31b3a3a3ad17c1e0b5f3"
        );
        assert_eq!(
            hex_of(&sig),
            "d3068040897e7eaaa647e45356dab05074e5d48f6a283ec72a5181421768783d"
        );
    }

    #[test]
    fn sha512_proof_is_64_bytes() {
        let (proof, sig) = client_proof_and_server_sig_sha512(
            "alice",
            "s3cret",
            "rOprNGfwEbeRWgbNEkqO",
            "rOprNGfwEbeRWgbNEkqOserver",
            b"saltSALTsaltSALT",
            4096,
        )
        .unwrap();
        assert_eq!(proof.len(), 64);
        assert_eq!(sig.len(), 64);
        let (p256, _) = client_proof_and_server_sig(
            "alice",
            "s3cret",
            "rOprNGfwEbeRWgbNEkqO",
            "rOprNGfwEbeRWgbNEkqOserver",
            b"saltSALTsaltSALT",
            4096,
        )
        .unwrap();
        assert_ne!(&proof[..32], p256.as_slice());
    }
}

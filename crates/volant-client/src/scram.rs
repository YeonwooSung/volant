//! Client-side SCRAM-SHA-256 proof computation (Phase 22).
//!
//! Matches broker AuthMessage construction (no channel binding).

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use rand::RngCore;
use sha2::{Digest, Sha256};
use volant_core::{Error, Result};

type HmacSha256 = Hmac<Sha256>;

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
    let salted = hi(password.as_bytes(), salt, iterations);
    let client_key = hmac_sha256(&salted, b"Client Key")?;
    let stored_key = sha256(&client_key);
    let server_key = hmac_sha256(&salted, b"Server Key")?;
    let auth_message =
        build_auth_message(username, client_nonce, combined_nonce, salt, iterations);
    let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes())?;
    let mut proof = vec![0u8; 32];
    for i in 0..32 {
        proof[i] = client_key[i] ^ client_signature[i];
    }
    let server_sig = hmac_sha256(&server_key, auth_message.as_bytes())?;
    Ok((proof, server_sig))
}

fn hi(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut out = [0u8; 32];
    pbkdf2_hmac::<Sha256>(password, salt, iterations, &mut out);
    out.to_vec()
}

fn sha256(data: &[u8]) -> Vec<u8> {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().to_vec()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
    let mut mac = HmacSha256::new_from_slice(key)
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
    let server_first = format!(
        "r={combined_nonce},s={},i={iterations}",
        B64.encode(salt)
    );
    let client_final_wo_proof = format!("c=biws,r={combined_nonce}");
    format!("{client_first_bare},{server_first},{client_final_wo_proof}")
}

//! SCRAM-SHA-256 credentials and crypto (Phase 22).
//!
//! Wire messages are Volant-binary; crypto follows RFC 5802 / 7677.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use hmac::{Hmac, Mac};
use parking_lot::RwLock;
use pbkdf2::pbkdf2_hmac;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use volant_core::{Error, Result};

type HmacSha256 = Hmac<Sha256>;

/// Default PBKDF2 iterations for new users.
pub const DEFAULT_ITERATIONS: u32 = 4096;

/// Stored SCRAM credential (never contains the password).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScramCredential {
    /// Base64 salt.
    pub salt_b64: String,
    /// Base64 StoredKey = H(ClientKey).
    pub stored_key_b64: String,
    /// Base64 ServerKey.
    pub server_key_b64: String,
    /// PBKDF2 iteration count.
    pub iterations: u32,
}

impl ScramCredential {
    fn salt(&self) -> Result<Vec<u8>> {
        B64.decode(&self.salt_b64)
            .map_err(|e| Error::Storage(format!("scram salt b64: {e}")))
    }

    fn stored_key(&self) -> Result<Vec<u8>> {
        B64.decode(&self.stored_key_b64)
            .map_err(|e| Error::Storage(format!("scram stored_key b64: {e}")))
    }

    fn server_key(&self) -> Result<Vec<u8>> {
        B64.decode(&self.server_key_b64)
            .map_err(|e| Error::Storage(format!("scram server_key b64: {e}")))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ScramFile {
    #[serde(default)]
    users: HashMap<String, ScramCredential>,
}

/// Pending first-message state for one connection.
#[derive(Debug, Clone)]
pub struct ScramChallenge {
    /// Username claimed by the client.
    pub username: String,
    /// Client nonce from ScramFirst.
    pub client_nonce: String,
    /// Combined client+server nonce.
    pub combined_nonce: String,
    /// Salt bytes sent to client.
    pub salt: Vec<u8>,
    /// Iterations sent to client.
    pub iterations: u32,
    /// True if the user exists (Final may still fail on proof).
    pub user_known: bool,
    /// Stored key when known.
    pub stored_key: Vec<u8>,
    /// Server key when known.
    pub server_key: Vec<u8>,
}

/// Durable SCRAM user store.
#[derive(Debug)]
pub struct ScramStore {
    path: PathBuf,
    users: RwLock<HashMap<String, ScramCredential>>,
}

impl ScramStore {
    /// Open under `data_dir/__scram` and load users.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let dir = data_dir.as_ref().join("__scram");
        fs::create_dir_all(&dir).map_err(|e| {
            Error::Storage(format!("create scram dir {}: {e}", dir.display()))
        })?;
        let path = dir.join("users.json");
        let users = if path.exists() {
            let mut f = File::open(&path).map_err(|e| {
                Error::Storage(format!("open scram store {}: {e}", path.display()))
            })?;
            let mut buf = String::new();
            f.read_to_string(&mut buf)
                .map_err(|e| Error::Storage(format!("read scram store: {e}")))?;
            if buf.trim().is_empty() {
                HashMap::new()
            } else {
                let file: ScramFile = serde_json::from_str(&buf)
                    .map_err(|e| Error::Storage(format!("parse scram store: {e}")))?;
                file.users
            }
        } else {
            HashMap::new()
        };
        Ok(Self {
            path,
            users: RwLock::new(users),
        })
    }

    fn persist(&self) -> Result<()> {
        let snap = ScramFile {
            users: self.users.read().clone(),
        };
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        let tmp = parent.join("users.json.tmp");
        let json = serde_json::to_string_pretty(&snap)
            .map_err(|e| Error::Storage(format!("encode scram store: {e}")))?;
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)
                .map_err(|e| Error::Storage(format!("open scram tmp: {e}")))?;
            f.write_all(json.as_bytes())
                .map_err(|e| Error::Storage(format!("write scram store: {e}")))?;
            f.sync_all()
                .map_err(|e| Error::Storage(format!("fsync scram store: {e}")))?;
        }
        fs::rename(&tmp, &self.path).map_err(|e| {
            Error::Storage(format!(
                "rename scram store {} -> {}: {e}",
                tmp.display(),
                self.path.display()
            ))
        })?;
        Ok(())
    }

    /// Number of registered users.
    pub fn user_count(&self) -> usize {
        self.users.read().len()
    }

    /// Whether any users are registered.
    pub fn has_users(&self) -> bool {
        self.user_count() > 0
    }

    /// List usernames (sorted).
    pub fn list_usernames(&self) -> Vec<String> {
        let mut names: Vec<_> = self.users.read().keys().cloned().collect();
        names.sort();
        names
    }

    /// Create or replace a user from a plaintext password.
    pub fn upsert_user(&self, username: &str, password: &str, iterations: u32) -> Result<()> {
        if username.is_empty() || username.contains(',') || username.contains('=') {
            return Err(Error::InvalidArgument(
                "invalid SCRAM username (empty or contains ,=)".into(),
            ));
        }
        if password.is_empty() {
            return Err(Error::InvalidArgument("empty SCRAM password".into()));
        }
        let iter = if iterations == 0 {
            DEFAULT_ITERATIONS
        } else {
            iterations
        };
        let cred = hash_password(password, iter)?;
        self.users.write().insert(username.to_owned(), cred);
        self.persist()
    }

    /// Delete a user. Returns whether it existed.
    pub fn delete_user(&self, username: &str) -> Result<bool> {
        let removed = self.users.write().remove(username).is_some();
        if removed {
            self.persist()?;
        }
        Ok(removed)
    }

    /// Begin SCRAM: build server-first fields + challenge.
    pub fn begin(&self, username: &str, client_nonce: &str) -> Result<(ScramChallenge, Vec<u8>, u32, String)> {
        if client_nonce.is_empty()
            || client_nonce.chars().any(|c| c == ',' || !c.is_ascii_graphic())
        {
            return Err(Error::InvalidArgument("invalid client_nonce".into()));
        }
        let server_nonce = random_nonce(18);
        let combined_nonce = format!("{client_nonce}{server_nonce}");

        let users = self.users.read();
        let (salt, iterations, stored_key, server_key, user_known) =
            if let Some(cred) = users.get(username) {
                (
                    cred.salt()?,
                    cred.iterations,
                    cred.stored_key()?,
                    cred.server_key()?,
                    true,
                )
            } else {
                // Anti-enumeration: random salt, default iterations, dummy keys.
                let mut salt = vec![0u8; 16];
                rand::thread_rng().fill_bytes(&mut salt);
                (
                    salt,
                    DEFAULT_ITERATIONS,
                    vec![0u8; 32],
                    vec![0u8; 32],
                    false,
                )
            };
        drop(users);

        let challenge = ScramChallenge {
            username: username.to_owned(),
            client_nonce: client_nonce.to_owned(),
            combined_nonce: combined_nonce.clone(),
            salt: salt.clone(),
            iterations,
            user_known,
            stored_key,
            server_key,
        };
        Ok((challenge, salt, iterations, combined_nonce))
    }

    /// Finish SCRAM: verify client proof, return server signature.
    pub fn finish(
        &self,
        challenge: &ScramChallenge,
        username: &str,
        combined_nonce: &str,
        client_proof: &[u8],
    ) -> Result<Vec<u8>> {
        if username != challenge.username || combined_nonce != challenge.combined_nonce {
            return Err(Error::InvalidArgument("scram nonce/user mismatch".into()));
        }
        if !challenge.user_known {
            return Err(Error::InvalidArgument("authentication failed".into()));
        }
        if client_proof.len() != 32 {
            return Err(Error::InvalidArgument("invalid client_proof length".into()));
        }

        let auth_message = build_auth_message(
            &challenge.username,
            &challenge.client_nonce,
            &challenge.combined_nonce,
            &challenge.salt,
            challenge.iterations,
        );

        let client_signature = hmac_sha256(&challenge.stored_key, auth_message.as_bytes())?;
        let mut client_key = vec![0u8; 32];
        for i in 0..32 {
            client_key[i] = client_proof[i] ^ client_signature[i];
        }
        let stored_key_check = sha256(&client_key);
        if !bool::from(stored_key_check.ct_eq(&challenge.stored_key)) {
            return Err(Error::InvalidArgument("authentication failed".into()));
        }
        hmac_sha256(&challenge.server_key, auth_message.as_bytes())
    }
}

/// Hash a password into a durable credential.
pub fn hash_password(password: &str, iterations: u32) -> Result<ScramCredential> {
    let mut salt = vec![0u8; 16];
    rand::thread_rng().fill_bytes(&mut salt);
    let (stored_key, server_key) = derive_keys(password, &salt, iterations)?;
    Ok(ScramCredential {
        salt_b64: B64.encode(&salt),
        stored_key_b64: B64.encode(&stored_key),
        server_key_b64: B64.encode(&server_key),
        iterations,
    })
}

/// Client-side: compute client proof and expected server signature.
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

fn derive_keys(password: &str, salt: &[u8], iterations: u32) -> Result<(Vec<u8>, Vec<u8>)> {
    let salted = hi(password.as_bytes(), salt, iterations);
    let client_key = hmac_sha256(&salted, b"Client Key")?;
    let stored_key = sha256(&client_key);
    let server_key = hmac_sha256(&salted, b"Server Key")?;
    Ok((stored_key, server_key))
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
    // c=biws is base64("n,,") — no channel binding.
    let client_final_wo_proof = format!("c=biws,r={combined_nonce}");
    format!("{client_first_bare},{server_first},{client_final_wo_proof}")
}

fn random_nonce(nbytes: usize) -> String {
    let mut buf = vec![0u8; nbytes];
    rand::thread_rng().fill_bytes(&mut buf);
    // Printable nonce without ',' .
    B64.encode(buf).replace(',', "A")
}

/// Generate a client nonce.
pub fn generate_client_nonce() -> String {
    random_nonce(16)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scram_roundtrip_proof() {
        let dir = std::env::temp_dir().join(format!(
            "volant-scram-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let store = ScramStore::open(&dir).unwrap();
        store.upsert_user("alice", "s3cret", 4096).unwrap();

        let client_nonce = generate_client_nonce();
        let (chal, salt, iter, combined) = store.begin("alice", &client_nonce).unwrap();
        let (proof, expected_sig) =
            client_proof_and_server_sig("alice", "s3cret", &client_nonce, &combined, &salt, iter)
                .unwrap();
        let server_sig = store
            .finish(&chal, "alice", &combined, &proof)
            .unwrap();
        assert_eq!(server_sig, expected_sig);

        let bad = store.finish(&chal, "alice", &combined, &[0u8; 32]);
        assert!(bad.is_err());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn unknown_user_fails_final() {
        let dir = std::env::temp_dir().join(format!(
            "volant-scram-unk-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let store = ScramStore::open(&dir).unwrap();
        let cn = generate_client_nonce();
        let (chal, salt, iter, combined) = store.begin("nobody", &cn).unwrap();
        let (proof, _) =
            client_proof_and_server_sig("nobody", "x", &cn, &combined, &salt, iter).unwrap();
        assert!(store.finish(&chal, "nobody", &combined, &proof).is_err());
        let _ = fs::remove_dir_all(&dir);
    }
}

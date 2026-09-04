//! SCRAM credentials and crypto (Phase 22 SHA-256; Phase 34 SHA-512).
//!
//! Wire messages for Volant-native are binary; Kafka SASL uses the same store.
//! Crypto follows RFC 5802 / 7677.

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
use sha2::{Digest, Sha256, Sha512};
use subtle::ConstantTimeEq;
use volant_core::{Error, Result};

type HmacSha256 = Hmac<Sha256>;
type HmacSha512 = Hmac<Sha512>;

/// Default PBKDF2 iterations for new users.
pub const DEFAULT_ITERATIONS: u32 = 4096;

/// Hash algorithm for SCRAM (Phase 34).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScramHash {
    /// SCRAM-SHA-256 (Phase 22 default).
    #[default]
    Sha256,
    /// SCRAM-SHA-512 (Phase 34).
    Sha512,
}

impl ScramHash {
    /// Output length of H() / HMAC / client proof.
    pub fn digest_len(self) -> usize {
        match self {
            Self::Sha256 => 32,
            Self::Sha512 => 64,
        }
    }

    /// Kafka SASL mechanism name.
    pub fn sasl_name(self) -> &'static str {
        match self {
            Self::Sha256 => "SCRAM-SHA-256",
            Self::Sha512 => "SCRAM-SHA-512",
        }
    }
}

/// Stored SCRAM credential for one hash algorithm (never contains the password).
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

/// Per-user credentials (one optional record per hash).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct UserCreds {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sha256: Option<ScramCredential>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sha512: Option<ScramCredential>,
}

impl UserCreds {
    fn get(&self, hash: ScramHash) -> Option<&ScramCredential> {
        match hash {
            ScramHash::Sha256 => self.sha256.as_ref(),
            ScramHash::Sha512 => self.sha512.as_ref(),
        }
    }

    fn set(&mut self, hash: ScramHash, cred: ScramCredential) {
        match hash {
            ScramHash::Sha256 => self.sha256 = Some(cred),
            ScramHash::Sha512 => self.sha512 = Some(cred),
        }
    }

    fn remove(&mut self, hash: ScramHash) -> Option<ScramCredential> {
        match hash {
            ScramHash::Sha256 => self.sha256.take(),
            ScramHash::Sha512 => self.sha512.take(),
        }
    }

    fn is_empty(&self) -> bool {
        self.sha256.is_none() && self.sha512.is_none()
    }

    fn infos(&self) -> Vec<(ScramHash, u32)> {
        let mut out = Vec::new();
        if let Some(c) = &self.sha256 {
            out.push((ScramHash::Sha256, c.iterations));
        }
        if let Some(c) = &self.sha512 {
            out.push((ScramHash::Sha512, c.iterations));
        }
        out
    }
}

/// On-disk user entry: legacy flat credential or multi-mechanism object.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum ScramUserRecord {
    /// Phase 22: single SHA-256 credential at the user key.
    Legacy(ScramCredential),
    /// Phase 34: per-mechanism credentials.
    Multi(UserCreds),
}

impl ScramUserRecord {
    fn into_creds(self) -> UserCreds {
        match self {
            Self::Legacy(c) => UserCreds {
                sha256: Some(c),
                sha512: None,
            },
            Self::Multi(u) => u,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ScramFile {
    #[serde(default)]
    users: HashMap<String, ScramUserRecord>,
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
    /// Hash algorithm for this exchange (Phase 34).
    pub hash: ScramHash,
}

/// Durable SCRAM user store.
#[derive(Debug)]
pub struct ScramStore {
    path: PathBuf,
    users: RwLock<HashMap<String, UserCreds>>,
}

impl ScramStore {
    /// Open under `data_dir/__scram` and load users.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let dir = data_dir.as_ref().join("__scram");
        fs::create_dir_all(&dir)
            .map_err(|e| Error::Storage(format!("create scram dir {}: {e}", dir.display())))?;
        let path = dir.join("users.json");
        let users = if path.exists() {
            let mut f = File::open(&path)
                .map_err(|e| Error::Storage(format!("open scram store {}: {e}", path.display())))?;
            let mut buf = String::new();
            f.read_to_string(&mut buf)
                .map_err(|e| Error::Storage(format!("read scram store: {e}")))?;
            if buf.trim().is_empty() {
                HashMap::new()
            } else {
                let file: ScramFile = serde_json::from_str(&buf)
                    .map_err(|e| Error::Storage(format!("parse scram store: {e}")))?;
                file.users
                    .into_iter()
                    .map(|(k, v)| (k, v.into_creds()))
                    .collect()
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
        let snap = {
            let guard = self.users.read();
            let mut users = HashMap::new();
            for (name, creds) in guard.iter() {
                users.insert(name.clone(), ScramUserRecord::Multi(creds.clone()));
            }
            ScramFile { users }
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
    ///
    /// Stores **both** SHA-256 and SHA-512 credentials (Phase 34) so either
    /// Kafka SASL mechanism works with the same password.
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
        let mut entry = UserCreds::default();
        entry.set(
            ScramHash::Sha256,
            hash_password_for(password, iter, ScramHash::Sha256)?,
        );
        entry.set(
            ScramHash::Sha512,
            hash_password_for(password, iter, ScramHash::Sha512)?,
        );
        self.users.write().insert(username.to_owned(), entry);
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

    /// Stored mechanisms for `username` (`None` if the user is unknown).
    pub fn describe_user(&self, username: &str) -> Option<Vec<(ScramHash, u32)>> {
        self.users.read().get(username).map(|c| c.infos())
    }

    /// All users and their stored mechanisms, sorted by name.
    pub fn describe_all(&self) -> Vec<(String, Vec<(ScramHash, u32)>)> {
        let users = self.users.read();
        let mut names: Vec<_> = users.keys().cloned().collect();
        names.sort();
        names
            .into_iter()
            .filter_map(|n| users.get(&n).map(|c| (n, c.infos())))
            .collect()
    }

    /// Whether `username` has a stored credential for `hash`.
    pub fn has_mechanism(&self, username: &str, hash: ScramHash) -> bool {
        self.users
            .read()
            .get(username)
            .and_then(|c| c.get(hash))
            .is_some()
    }

    /// Create or replace one mechanism from Kafka `SaltedPassword = Hi(password, salt, i)`.
    ///
    /// Does **not** take a plaintext password. Native [`Self::upsert_user`] is
    /// unchanged and still writes both hashes from plaintext.
    pub fn upsert_from_salted(
        &self,
        username: &str,
        hash: ScramHash,
        iterations: u32,
        salt: &[u8],
        salted_password: &[u8],
    ) -> Result<()> {
        if username.is_empty() || username.contains(',') || username.contains('=') {
            return Err(Error::InvalidArgument(
                "invalid SCRAM username (empty or contains ,=)".into(),
            ));
        }
        if iterations == 0 {
            return Err(Error::InvalidArgument(
                "invalid SCRAM iterations (must be > 0)".into(),
            ));
        }
        if salt.is_empty() || salted_password.is_empty() {
            return Err(Error::InvalidArgument(
                "empty SCRAM salt or saltedPassword".into(),
            ));
        }
        let (stored_key, server_key) = keys_from_salted(salted_password, hash)?;
        let cred = ScramCredential {
            salt_b64: B64.encode(salt),
            stored_key_b64: B64.encode(&stored_key),
            server_key_b64: B64.encode(&server_key),
            iterations,
        };
        {
            let mut users = self.users.write();
            users
                .entry(username.to_owned())
                .or_default()
                .set(hash, cred);
        }
        self.persist()
    }

    /// Remove one mechanism. Deletes the user if none remain.
    ///
    /// Returns whether that user/mechanism existed.
    pub fn delete_mechanism(&self, username: &str, hash: ScramHash) -> Result<bool> {
        let removed = {
            let mut users = self.users.write();
            let Some(entry) = users.get_mut(username) else {
                return Ok(false);
            };
            if entry.remove(hash).is_none() {
                return Ok(false);
            }
            if entry.is_empty() {
                users.remove(username);
            }
            true
        };
        if removed {
            self.persist()?;
        }
        Ok(removed)
    }

    /// Begin SCRAM-SHA-256 (Volant-native + default).
    pub fn begin(
        &self,
        username: &str,
        client_nonce: &str,
    ) -> Result<(ScramChallenge, Vec<u8>, u32, String)> {
        self.begin_with_hash(username, client_nonce, ScramHash::Sha256)
    }

    /// Begin SCRAM with an explicit hash (Phase 34 Kafka SCRAM-SHA-512).
    pub fn begin_with_hash(
        &self,
        username: &str,
        client_nonce: &str,
        hash: ScramHash,
    ) -> Result<(ScramChallenge, Vec<u8>, u32, String)> {
        if client_nonce.is_empty()
            || client_nonce
                .chars()
                .any(|c| c == ',' || !c.is_ascii_graphic())
        {
            return Err(Error::InvalidArgument("invalid client_nonce".into()));
        }
        let server_nonce = random_nonce(18);
        let combined_nonce = format!("{client_nonce}{server_nonce}");
        let dig_len = hash.digest_len();

        let users = self.users.read();
        let (salt, iterations, stored_key, server_key, user_known) =
            if let Some(creds) = users.get(username) {
                if let Some(cred) = creds.get(hash) {
                    (
                        cred.salt()?,
                        cred.iterations,
                        cred.stored_key()?,
                        cred.server_key()?,
                        true,
                    )
                } else {
                    // User exists but not for this hash (e.g. legacy SHA-256 only).
                    let mut salt = vec![0u8; 16];
                    rand::thread_rng().fill_bytes(&mut salt);
                    (
                        salt,
                        DEFAULT_ITERATIONS,
                        vec![0u8; dig_len],
                        vec![0u8; dig_len],
                        false,
                    )
                }
            } else {
                let mut salt = vec![0u8; 16];
                rand::thread_rng().fill_bytes(&mut salt);
                (
                    salt,
                    DEFAULT_ITERATIONS,
                    vec![0u8; dig_len],
                    vec![0u8; dig_len],
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
            hash,
        };
        Ok((challenge, salt, iterations, combined_nonce))
    }

    /// Verify a plaintext password against a stored SCRAM credential (PLAIN).
    ///
    /// Tries SHA-256 first, then SHA-512.
    pub fn verify_password(&self, username: &str, password: &str) -> bool {
        if username.is_empty() || password.is_empty() {
            return false;
        }
        let users = self.users.read();
        let Some(creds) = users.get(username) else {
            return false;
        };
        for hash in [ScramHash::Sha256, ScramHash::Sha512] {
            let Some(cred) = creds.get(hash) else {
                continue;
            };
            let Ok(salt) = cred.salt() else {
                continue;
            };
            let Ok(stored_key) = cred.stored_key() else {
                continue;
            };
            let Ok((derived, _)) = derive_keys(password, &salt, cred.iterations, hash) else {
                continue;
            };
            if bool::from(derived.ct_eq(&stored_key)) {
                return true;
            }
        }
        false
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
        let dig = challenge.hash.digest_len();
        if client_proof.len() != dig {
            return Err(Error::InvalidArgument("invalid client_proof length".into()));
        }

        let auth_message = build_auth_message(
            &challenge.username,
            &challenge.client_nonce,
            &challenge.combined_nonce,
            &challenge.salt,
            challenge.iterations,
        );

        let client_signature = hmac_hash(
            challenge.hash,
            &challenge.stored_key,
            auth_message.as_bytes(),
        )?;
        let mut client_key = vec![0u8; dig];
        for i in 0..dig {
            client_key[i] = client_proof[i] ^ client_signature[i];
        }
        let stored_key_check = hash_digest(challenge.hash, &client_key);
        if !bool::from(stored_key_check.ct_eq(&challenge.stored_key)) {
            return Err(Error::InvalidArgument("authentication failed".into()));
        }
        hmac_hash(
            challenge.hash,
            &challenge.server_key,
            auth_message.as_bytes(),
        )
    }
}

/// Hash a password into a durable SHA-256 credential (compat helper).
pub fn hash_password(password: &str, iterations: u32) -> Result<ScramCredential> {
    hash_password_for(password, iterations, ScramHash::Sha256)
}

/// Hash a password for a specific SCRAM hash algorithm.
pub fn hash_password_for(
    password: &str,
    iterations: u32,
    hash: ScramHash,
) -> Result<ScramCredential> {
    let mut salt = vec![0u8; 16];
    rand::thread_rng().fill_bytes(&mut salt);
    let (stored_key, server_key) = derive_keys(password, &salt, iterations, hash)?;
    Ok(ScramCredential {
        salt_b64: B64.encode(&salt),
        stored_key_b64: B64.encode(&stored_key),
        server_key_b64: B64.encode(&server_key),
        iterations,
    })
}

/// Client-side: compute client proof and expected server signature (SHA-256).
pub fn client_proof_and_server_sig(
    username: &str,
    password: &str,
    client_nonce: &str,
    combined_nonce: &str,
    salt: &[u8],
    iterations: u32,
) -> Result<(Vec<u8>, Vec<u8>)> {
    client_proof_and_server_sig_for(
        ScramHash::Sha256,
        username,
        password,
        client_nonce,
        combined_nonce,
        salt,
        iterations,
    )
}

/// Client-side proof for an explicit hash (Phase 34).
pub fn client_proof_and_server_sig_for(
    hash: ScramHash,
    username: &str,
    password: &str,
    client_nonce: &str,
    combined_nonce: &str,
    salt: &[u8],
    iterations: u32,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let dig = hash.digest_len();
    let salted = hi(password.as_bytes(), salt, iterations, hash);
    let client_key = hmac_hash(hash, &salted, b"Client Key")?;
    let stored_key = hash_digest(hash, &client_key);
    let server_key = hmac_hash(hash, &salted, b"Server Key")?;
    let auth_message = build_auth_message(username, client_nonce, combined_nonce, salt, iterations);
    let client_signature = hmac_hash(hash, &stored_key, auth_message.as_bytes())?;
    let mut proof = vec![0u8; dig];
    for i in 0..dig {
        proof[i] = client_key[i] ^ client_signature[i];
    }
    let server_sig = hmac_hash(hash, &server_key, auth_message.as_bytes())?;
    Ok((proof, server_sig))
}

fn derive_keys(
    password: &str,
    salt: &[u8],
    iterations: u32,
    hash: ScramHash,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let salted = hi(password.as_bytes(), salt, iterations, hash);
    keys_from_salted(&salted, hash)
}

/// Derive StoredKey/ServerKey from Kafka `SaltedPassword = Hi(password, salt, i)`.
fn keys_from_salted(salted_password: &[u8], hash: ScramHash) -> Result<(Vec<u8>, Vec<u8>)> {
    let client_key = hmac_hash(hash, salted_password, b"Client Key")?;
    let stored_key = hash_digest(hash, &client_key);
    let server_key = hmac_hash(hash, salted_password, b"Server Key")?;
    Ok((stored_key, server_key))
}

/// RFC 5802 `Hi(password, salt, i)` (PBKDF2). Kafka Alter upsert `saltedPassword`.
pub fn salted_password_for(
    password: &str,
    salt: &[u8],
    iterations: u32,
    hash: ScramHash,
) -> Vec<u8> {
    hi(password.as_bytes(), salt, iterations, hash)
}

fn hi(password: &[u8], salt: &[u8], iterations: u32, hash: ScramHash) -> Vec<u8> {
    match hash {
        ScramHash::Sha256 => {
            let mut out = [0u8; 32];
            pbkdf2_hmac::<Sha256>(password, salt, iterations, &mut out);
            out.to_vec()
        }
        ScramHash::Sha512 => {
            let mut out = [0u8; 64];
            pbkdf2_hmac::<Sha512>(password, salt, iterations, &mut out);
            out.to_vec()
        }
    }
}

fn hash_digest(hash: ScramHash, data: &[u8]) -> Vec<u8> {
    match hash {
        ScramHash::Sha256 => {
            let mut h = Sha256::new();
            h.update(data);
            h.finalize().to_vec()
        }
        ScramHash::Sha512 => {
            let mut h = Sha512::new();
            h.update(data);
            h.finalize().to_vec()
        }
    }
}

fn hmac_hash(hash: ScramHash, key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
    match hash {
        ScramHash::Sha256 => {
            let mut mac = HmacSha256::new_from_slice(key)
                .map_err(|e| Error::InvalidArgument(format!("hmac key: {e}")))?;
            mac.update(data);
            Ok(mac.finalize().into_bytes().to_vec())
        }
        ScramHash::Sha512 => {
            let mut mac = HmacSha512::new_from_slice(key)
                .map_err(|e| Error::InvalidArgument(format!("hmac key: {e}")))?;
            mac.update(data);
            Ok(mac.finalize().into_bytes().to_vec())
        }
    }
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
    // c=biws is base64("n,,") — no channel binding.
    let client_final_wo_proof = format!("c=biws,r={combined_nonce}");
    format!("{client_first_bare},{server_first},{client_final_wo_proof}")
}

fn random_nonce(nbytes: usize) -> String {
    let mut buf = vec![0u8; nbytes];
    rand::thread_rng().fill_bytes(&mut buf);
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
    fn scram_sha256_roundtrip_proof() {
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
        assert_eq!(chal.hash, ScramHash::Sha256);
        let (proof, expected_sig) =
            client_proof_and_server_sig("alice", "s3cret", &client_nonce, &combined, &salt, iter)
                .unwrap();
        let server_sig = store.finish(&chal, "alice", &combined, &proof).unwrap();
        assert_eq!(server_sig, expected_sig);

        let bad = store.finish(&chal, "alice", &combined, &[0u8; 32]);
        assert!(bad.is_err());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn scram_sha512_roundtrip_proof() {
        let dir = std::env::temp_dir().join(format!(
            "volant-scram512-{}-{}",
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
        let (chal, salt, iter, combined) = store
            .begin_with_hash("alice", &client_nonce, ScramHash::Sha512)
            .unwrap();
        assert_eq!(chal.hash, ScramHash::Sha512);
        let (proof, expected_sig) = client_proof_and_server_sig_for(
            ScramHash::Sha512,
            "alice",
            "s3cret",
            &client_nonce,
            &combined,
            &salt,
            iter,
        )
        .unwrap();
        assert_eq!(proof.len(), 64);
        let server_sig = store.finish(&chal, "alice", &combined, &proof).unwrap();
        assert_eq!(server_sig, expected_sig);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn both_mechanisms_after_upsert() {
        let dir = std::env::temp_dir().join(format!(
            "volant-scram-both-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let store = ScramStore::open(&dir).unwrap();
        store.upsert_user("bob", "pw", 0).unwrap();
        assert!(store.verify_password("bob", "pw"));
        assert!(!store.verify_password("bob", "wrong"));

        for hash in [ScramHash::Sha256, ScramHash::Sha512] {
            let cn = generate_client_nonce();
            let (chal, salt, iter, combined) = store.begin_with_hash("bob", &cn, hash).unwrap();
            let (proof, _) =
                client_proof_and_server_sig_for(hash, "bob", "pw", &cn, &combined, &salt, iter)
                    .unwrap();
            assert!(store.finish(&chal, "bob", &combined, &proof).is_ok());
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn legacy_users_json_loads_as_sha256() {
        let dir = std::env::temp_dir().join(format!(
            "volant-scram-legacy-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        let scram_dir = dir.join("__scram");
        fs::create_dir_all(&scram_dir).unwrap();
        let cred = hash_password("legacy-pass", 4096).unwrap();
        let json = serde_json::json!({
            "users": {
                "legacy": {
                    "salt_b64": cred.salt_b64,
                    "stored_key_b64": cred.stored_key_b64,
                    "server_key_b64": cred.server_key_b64,
                    "iterations": cred.iterations
                }
            }
        });
        fs::write(
            scram_dir.join("users.json"),
            serde_json::to_string_pretty(&json).unwrap(),
        )
        .unwrap();

        let store = ScramStore::open(&dir).unwrap();
        assert!(store.verify_password("legacy", "legacy-pass"));
        let cn = generate_client_nonce();
        let (chal, salt, iter, combined) = store.begin("legacy", &cn).unwrap();
        let (proof, _) =
            client_proof_and_server_sig("legacy", "legacy-pass", &cn, &combined, &salt, iter)
                .unwrap();
        assert!(store.finish(&chal, "legacy", &combined, &proof).is_ok());

        // SHA-512 not available until re-upsert.
        let (chal512, _, _, _) = store
            .begin_with_hash("legacy", &cn, ScramHash::Sha512)
            .unwrap();
        assert!(!chal512.user_known);

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

    #[test]
    fn describe_after_native_upsert_lists_both_mechanisms() {
        let dir = std::env::temp_dir().join(format!(
            "volant-scram-desc-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let store = ScramStore::open(&dir).unwrap();
        store.upsert_user("alice", "s3cret", 0).unwrap();
        let infos = store.describe_user("alice").unwrap();
        assert_eq!(
            infos,
            vec![
                (ScramHash::Sha256, DEFAULT_ITERATIONS),
                (ScramHash::Sha512, DEFAULT_ITERATIONS)
            ]
        );
        assert!(store.describe_user("nobody").is_none());
        let all = store.describe_all();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].0, "alice");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn upsert_from_salted_then_delete_mechanism() {
        let dir = std::env::temp_dir().join(format!(
            "volant-scram-salted-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let store = ScramStore::open(&dir).unwrap();
        let salt = b"0123456789abcdef";
        let salted = salted_password_for("pw", salt, 4096, ScramHash::Sha256);
        store
            .upsert_from_salted("carol", ScramHash::Sha256, 4096, salt, &salted)
            .unwrap();
        assert_eq!(
            store.describe_user("carol").unwrap(),
            vec![(ScramHash::Sha256, 4096)]
        );
        assert!(store.verify_password("carol", "pw"));
        assert!(store.has_mechanism("carol", ScramHash::Sha256));
        assert!(!store.has_mechanism("carol", ScramHash::Sha512));

        assert!(store.delete_mechanism("carol", ScramHash::Sha256).unwrap());
        assert!(store.describe_user("carol").is_none());
        assert!(!store.delete_mechanism("carol", ScramHash::Sha256).unwrap());
        let _ = fs::remove_dir_all(&dir);
    }
}

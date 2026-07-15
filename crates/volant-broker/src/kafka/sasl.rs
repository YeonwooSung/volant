//! Kafka SASL handshake state (PLAIN + SCRAM-SHA-256) — Phase 30.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use volant_core::{Error, Result};

use crate::broker::Broker;
use crate::scram::ScramChallenge;

/// Mechanisms advertised by SaslHandshake.
pub const MECHANISMS: &[&str] = &["PLAIN", "SCRAM-SHA-256"];

/// Selected SASL mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaslMechanism {
    /// RFC 4616 PLAIN (password checked against SCRAM store).
    Plain,
    /// SCRAM-SHA-256 (RFC 5802 / 7677).
    ScramSha256,
}

impl SaslMechanism {
    /// Parse mechanism name (case-sensitive Kafka convention).
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "PLAIN" => Some(Self::Plain),
            "SCRAM-SHA-256" => Some(Self::ScramSha256),
            _ => None,
        }
    }

    /// Wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
        }
    }
}

/// Per-connection SASL state machine.
#[derive(Debug, Default)]
pub enum SaslState {
    /// No mechanism selected yet.
    #[default]
    Idle,
    /// Handshake succeeded; waiting for first authenticate bytes.
    Selected(SaslMechanism),
    /// SCRAM server-first sent; waiting for client-final.
    ScramPending(ScramChallenge),
    /// Authentication complete (principal stored on connection).
    Done,
}

/// Outcome of one SaslAuthenticate step.
#[derive(Debug)]
pub struct AuthStep {
    /// Bytes to return in the SaslAuthenticate response.
    pub auth_bytes: Vec<u8>,
    /// Set when authentication just completed.
    pub principal: Option<String>,
    /// True when this step failed (caller should set error_code 58).
    pub failed: bool,
    /// Optional error message for the response.
    pub error_message: Option<String>,
}

/// Process SaslAuthenticate auth_bytes for the current state.
pub fn authenticate_step(
    broker: &Broker,
    state: &mut SaslState,
    auth_bytes: &[u8],
) -> Result<AuthStep> {
    match state {
        SaslState::Idle | SaslState::Done => Ok(AuthStep {
            auth_bytes: Vec::new(),
            principal: None,
            failed: true,
            error_message: Some("SASL handshake required first".into()),
        }),
        SaslState::Selected(SaslMechanism::Plain) => {
            let result = plain_authenticate(broker, auth_bytes);
            if result.principal.is_some() {
                *state = SaslState::Done;
            }
            Ok(result)
        }
        SaslState::Selected(SaslMechanism::ScramSha256) => {
            let (step, next) = scram_client_first(broker, auth_bytes)?;
            *state = next;
            Ok(step)
        }
        SaslState::ScramPending(chal) => {
            let chal = chal.clone();
            let step = scram_client_final(broker, &chal, auth_bytes);
            if step.principal.is_some() {
                *state = SaslState::Done;
            } else if step.failed {
                *state = SaslState::Idle;
            }
            Ok(step)
        }
    }
}

fn plain_authenticate(broker: &Broker, auth_bytes: &[u8]) -> AuthStep {
    // RFC 4616: [authzid] NUL username NUL password
    let parts: Vec<&[u8]> = auth_bytes.split(|&b| b == 0).collect();
    // Accept either ["", user, pass] or [authzid, user, pass]
    let (user, pass) = match parts.as_slice() {
        [_, user, pass] => (*user, *pass),
        [user, pass] => (*user, *pass),
        _ => {
            return AuthStep {
                auth_bytes: Vec::new(),
                principal: None,
                failed: true,
                error_message: Some("invalid PLAIN message".into()),
            };
        }
    };
    let username = match std::str::from_utf8(user) {
        Ok(s) if !s.is_empty() => s,
        _ => {
            return AuthStep {
                auth_bytes: Vec::new(),
                principal: None,
                failed: true,
                error_message: Some("invalid PLAIN username".into()),
            };
        }
    };
    let password = match std::str::from_utf8(pass) {
        Ok(s) => s,
        Err(_) => {
            return AuthStep {
                auth_bytes: Vec::new(),
                principal: None,
                failed: true,
                error_message: Some("invalid PLAIN password".into()),
            };
        }
    };
    if broker.scram().verify_password(username, password) {
        AuthStep {
            auth_bytes: Vec::new(),
            principal: Some(username.to_owned()),
            failed: false,
            error_message: None,
        }
    } else {
        AuthStep {
            auth_bytes: Vec::new(),
            principal: None,
            failed: true,
            error_message: Some("authentication failed".into()),
        }
    }
}

fn scram_client_first(
    broker: &Broker,
    auth_bytes: &[u8],
) -> Result<(AuthStep, SaslState)> {
    let msg = std::str::from_utf8(auth_bytes)
        .map_err(|_| Error::Protocol("SCRAM client-first not utf8".into()))?;
    // Forms: "n,,n=user,r=nonce" or bare "n=user,r=nonce"
    let bare = if let Some(rest) = msg.strip_prefix("n,,") {
        rest
    } else if msg.starts_with("y,,") || msg.starts_with("p=") {
        return Ok((
            AuthStep {
                auth_bytes: b"e=channel-binding-not-supported".to_vec(),
                principal: None,
                failed: true,
                error_message: Some("channel binding not supported".into()),
            },
            SaslState::Idle,
        ));
    } else {
        msg
    };
    let mut username = None;
    let mut client_nonce = None;
    for part in bare.split(',') {
        if let Some(u) = part.strip_prefix("n=") {
            username = Some(sasl_decode_name(u));
        } else if let Some(r) = part.strip_prefix("r=") {
            client_nonce = Some(r.to_owned());
        }
    }
    let (username, client_nonce) = match (username, client_nonce) {
        (Some(u), Some(r)) if !u.is_empty() && !r.is_empty() => (u, r),
        _ => {
            return Ok((
                AuthStep {
                    auth_bytes: b"e=invalid-encoding".to_vec(),
                    principal: None,
                    failed: true,
                    error_message: Some("invalid SCRAM client-first".into()),
                },
                SaslState::Idle,
            ));
        }
    };

    match broker.scram().begin(&username, &client_nonce) {
        Ok((chal, salt, iterations, combined_nonce)) => {
            let server_first = format!(
                "r={combined_nonce},s={},i={iterations}",
                B64.encode(&salt)
            );
            Ok((
                AuthStep {
                    auth_bytes: server_first.into_bytes(),
                    principal: None,
                    failed: false,
                    error_message: None,
                },
                SaslState::ScramPending(chal),
            ))
        }
        Err(e) => Ok((
            AuthStep {
                auth_bytes: b"e=invalid-encoding".to_vec(),
                principal: None,
                failed: true,
                error_message: Some(e.to_string()),
            },
            SaslState::Idle,
        )),
    }
}

fn scram_client_final(broker: &Broker, chal: &ScramChallenge, auth_bytes: &[u8]) -> AuthStep {
    let msg = match std::str::from_utf8(auth_bytes) {
        Ok(s) => s,
        Err(_) => {
            return AuthStep {
                auth_bytes: b"e=invalid-encoding".to_vec(),
                principal: None,
                failed: true,
                error_message: Some("invalid SCRAM client-final".into()),
            };
        }
    };
    let mut combined_nonce = None;
    let mut proof_b64 = None;
    for part in msg.split(',') {
        if let Some(r) = part.strip_prefix("r=") {
            combined_nonce = Some(r);
        } else if let Some(p) = part.strip_prefix("p=") {
            proof_b64 = Some(p);
        }
    }
    let (combined_nonce, proof_b64) = match (combined_nonce, proof_b64) {
        (Some(r), Some(p)) => (r, p),
        _ => {
            return AuthStep {
                auth_bytes: b"e=invalid-encoding".to_vec(),
                principal: None,
                failed: true,
                error_message: Some("invalid SCRAM client-final".into()),
            };
        }
    };
    let proof = match B64.decode(proof_b64) {
        Ok(p) => p,
        Err(_) => {
            return AuthStep {
                auth_bytes: b"e=invalid-encoding".to_vec(),
                principal: None,
                failed: true,
                error_message: Some("invalid client proof encoding".into()),
            };
        }
    };
    match broker
        .scram()
        .finish(chal, &chal.username, combined_nonce, &proof)
    {
        Ok(server_sig) => AuthStep {
            auth_bytes: format!("v={}", B64.encode(&server_sig)).into_bytes(),
            principal: Some(chal.username.clone()),
            failed: false,
            error_message: None,
        },
        Err(_) => AuthStep {
            auth_bytes: b"e=invalid-proof".to_vec(),
            principal: None,
            failed: true,
            error_message: Some("authentication failed".into()),
        },
    }
}

/// Decode SCRAM username (=2C / =3D escapes).
fn sasl_decode_name(s: &str) -> String {
    s.replace("=2C", ",").replace("=3D", "=")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scram::client_proof_and_server_sig;
    use volant_storage::StorageConfig;

    fn broker_with_user(user: &str, pass: &str) -> Broker {
        let dir = std::env::temp_dir().join(format!(
            "volant-sasl-unit-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let b = Broker::new(StorageConfig {
            data_dir: dir,
            ..StorageConfig::default()
        });
        b.upsert_scram_user(user, pass).unwrap();
        b
    }

    #[test]
    fn plain_roundtrip() {
        let b = broker_with_user("alice", "s3cret");
        let mut state = SaslState::Selected(SaslMechanism::Plain);
        let msg = b"\0alice\0s3cret";
        let step = authenticate_step(&b, &mut state, msg).unwrap();
        assert!(!step.failed);
        assert_eq!(step.principal.as_deref(), Some("alice"));
        assert!(matches!(state, SaslState::Done));
    }

    #[test]
    fn plain_bad_password() {
        let b = broker_with_user("alice", "s3cret");
        let mut state = SaslState::Selected(SaslMechanism::Plain);
        let step = authenticate_step(&b, &mut state, b"\0alice\0wrong").unwrap();
        assert!(step.failed);
        assert!(step.principal.is_none());
    }

    #[test]
    fn scram_sha256_roundtrip() {
        let b = broker_with_user("bob", "hunter2");
        let mut state = SaslState::Selected(SaslMechanism::ScramSha256);
        let client_nonce = "rOprNGfwEbeRWgbNEkqO";
        let first = format!("n,,n=bob,r={client_nonce}");
        let step1 = authenticate_step(&b, &mut state, first.as_bytes()).unwrap();
        assert!(!step1.failed);
        assert!(step1.principal.is_none());
        let server_first = std::str::from_utf8(&step1.auth_bytes).unwrap();
        // r=combined,s=salt,i=iter
        let mut combined = None;
        let mut salt_b64 = None;
        let mut iter = None;
        for part in server_first.split(',') {
            if let Some(r) = part.strip_prefix("r=") {
                combined = Some(r.to_owned());
            } else if let Some(s) = part.strip_prefix("s=") {
                salt_b64 = Some(s.to_owned());
            } else if let Some(i) = part.strip_prefix("i=") {
                iter = Some(i.parse::<u32>().unwrap());
            }
        }
        let combined = combined.unwrap();
        let salt = B64.decode(salt_b64.unwrap()).unwrap();
        let iterations = iter.unwrap();
        let (proof, _sig) = client_proof_and_server_sig(
            "bob",
            "hunter2",
            client_nonce,
            &combined,
            &salt,
            iterations,
        )
        .unwrap();
        let final_msg = format!("c=biws,r={combined},p={}", B64.encode(&proof));
        let step2 = authenticate_step(&b, &mut state, final_msg.as_bytes()).unwrap();
        assert!(!step2.failed, "{:?}", step2.error_message);
        assert_eq!(step2.principal.as_deref(), Some("bob"));
        assert!(std::str::from_utf8(&step2.auth_bytes)
            .unwrap()
            .starts_with("v="));
        assert!(matches!(state, SaslState::Done));
    }
}

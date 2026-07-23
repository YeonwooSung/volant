//! Principal-based access control lists (Phase 20/21).

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use volant_core::{Error, Result};

/// Resource category for an ACL binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
#[repr(u8)]
pub enum ResourceType {
    /// Topic resource.
    Topic = 0,
    /// Consumer group resource.
    Group = 1,
    /// Cluster-scoped resource (`volant`).
    Cluster = 2,
    /// User resource (Kafka ACL v3 / SCRAM credential subject). Stored and
    /// listed via Kafka Describe/Create/DeleteAcls; not consulted on the
    /// produce/fetch authorize path today.
    User = 3,
}

impl ResourceType {
    /// Parse wire `u8`.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Topic),
            1 => Some(Self::Group),
            2 => Some(Self::Cluster),
            3 => Some(Self::User),
            _ => None,
        }
    }

    /// Wire value.
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Parse JSON / CLI name.
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "topic" => Ok(Self::Topic),
            "group" => Ok(Self::Group),
            "cluster" => Ok(Self::Cluster),
            "user" => Ok(Self::User),
            other => Err(Error::InvalidArgument(format!(
                "unknown resource_type '{other}'"
            ))),
        }
    }

    /// Stable name for display / JSON.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Topic => "Topic",
            Self::Group => "Group",
            Self::Cluster => "Cluster",
            Self::User => "User",
        }
    }
}

/// Operation gated by an ACL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
#[repr(u8)]
pub enum AclOperation {
    /// Matches any operation.
    All = 0,
    /// Read / fetch / consume.
    Read = 1,
    /// Write / produce.
    Write = 2,
    /// Create resource.
    Create = 3,
    /// Delete resource.
    Delete = 4,
    /// Describe / metadata.
    Describe = 5,
    /// Alter config / partitions.
    Alter = 6,
    /// Inter-broker / cluster action (reserved).
    ClusterAction = 7,
}

impl AclOperation {
    /// Parse wire `u8`.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::All),
            1 => Some(Self::Read),
            2 => Some(Self::Write),
            3 => Some(Self::Create),
            4 => Some(Self::Delete),
            5 => Some(Self::Describe),
            6 => Some(Self::Alter),
            7 => Some(Self::ClusterAction),
            _ => None,
        }
    }

    /// Wire value.
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Parse JSON / CLI name.
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "all" => Ok(Self::All),
            "read" => Ok(Self::Read),
            "write" => Ok(Self::Write),
            "create" => Ok(Self::Create),
            "delete" => Ok(Self::Delete),
            "describe" => Ok(Self::Describe),
            "alter" => Ok(Self::Alter),
            "clusteraction" | "cluster_action" => Ok(Self::ClusterAction),
            other => Err(Error::InvalidArgument(format!(
                "unknown operation '{other}'"
            ))),
        }
    }

    /// Stable name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::All => "All",
            Self::Read => "Read",
            Self::Write => "Write",
            Self::Create => "Create",
            Self::Delete => "Delete",
            Self::Describe => "Describe",
            Self::Alter => "Alter",
            Self::ClusterAction => "ClusterAction",
        }
    }

    fn matches(self, required: AclOperation) -> bool {
        self == AclOperation::All || self == required
    }
}

/// Allow or deny.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
#[repr(u8)]
pub enum AclPermission {
    /// Explicit deny (wins over allow).
    Deny = 0,
    /// Explicit allow.
    Allow = 1,
}

impl AclPermission {
    /// Parse wire `u8`.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Deny),
            1 => Some(Self::Allow),
            _ => None,
        }
    }

    /// Wire value.
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Parse name.
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "deny" => Ok(Self::Deny),
            "allow" => Ok(Self::Allow),
            other => Err(Error::InvalidArgument(format!(
                "unknown permission '{other}'"
            ))),
        }
    }

    /// Stable name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Deny => "Deny",
            Self::Allow => "Allow",
        }
    }
}

/// One ACL binding.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AclEntry {
    /// Principal name, or `*` for any.
    pub principal: String,
    /// Resource category.
    pub resource_type: ResourceType,
    /// Resource name, or `*` for any.
    pub resource: String,
    /// Gated operation.
    pub operation: AclOperation,
    /// Allow or deny.
    pub permission: AclPermission,
}

impl AclEntry {
    fn principal_matches(&self, principal: &str) -> bool {
        self.principal == "*" || self.principal == principal
    }

    fn resource_matches(&self, name: &str) -> bool {
        self.resource == "*" || self.resource == name
    }

    fn matches(
        &self,
        principal: &str,
        resource_type: ResourceType,
        resource: &str,
        operation: AclOperation,
    ) -> bool {
        self.principal_matches(principal)
            && self.resource_type == resource_type
            && self.resource_matches(resource)
            && self.operation.matches(operation)
    }
}

/// Canonical cluster resource name used in ACL checks.
pub const CLUSTER_RESOURCE: &str = "volant";

/// Durable ACL snapshot on disk (Phase 21).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AclSnapshot {
    /// Whether enforcement is on.
    #[serde(default)]
    pub enabled: bool,
    /// ACL bindings.
    #[serde(default)]
    pub entries: Vec<AclEntry>,
}

/// File-backed ACL store under `{data_dir}/__acls/acls.json`.
#[derive(Debug, Clone)]
pub struct AclStore {
    path: PathBuf,
}

impl AclStore {
    /// Open (create dir) under `data_dir/__acls`.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let dir = data_dir.as_ref().join("__acls");
        fs::create_dir_all(&dir).map_err(|e| {
            Error::Storage(format!("create acl dir {}: {e}", dir.display()))
        })?;
        Ok(Self {
            path: dir.join("acls.json"),
        })
    }

    /// Path to the JSON file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Load snapshot; empty defaults if missing.
    pub fn load(&self) -> Result<AclSnapshot> {
        if !self.path.exists() {
            return Ok(AclSnapshot::default());
        }
        let mut f = File::open(&self.path).map_err(|e| {
            Error::Storage(format!("open acl store {}: {e}", self.path.display()))
        })?;
        let mut buf = String::new();
        f.read_to_string(&mut buf)
            .map_err(|e| Error::Storage(format!("read acl store: {e}")))?;
        if buf.trim().is_empty() {
            return Ok(AclSnapshot::default());
        }
        // Accept Phase 20 bare array or Phase 21 snapshot object.
        if buf.trim_start().starts_with('[') {
            let entries: Vec<AclEntry> = serde_json::from_str(&buf)
                .map_err(|e| Error::Storage(format!("parse acl array: {e}")))?;
            return Ok(AclSnapshot {
                enabled: !entries.is_empty(),
                entries,
            });
        }
        serde_json::from_str(&buf).map_err(|e| Error::Storage(format!("parse acl store: {e}")))
    }

    /// Atomically persist snapshot.
    pub fn save(&self, snap: &AclSnapshot) -> Result<()> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        let tmp = parent.join("acls.json.tmp");
        let json = serde_json::to_string_pretty(snap)
            .map_err(|e| Error::Storage(format!("encode acl store: {e}")))?;
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)
                .map_err(|e| Error::Storage(format!("open acl tmp: {e}")))?;
            f.write_all(json.as_bytes())
                .map_err(|e| Error::Storage(format!("write acl store: {e}")))?;
            f.sync_all()
                .map_err(|e| Error::Storage(format!("fsync acl store: {e}")))?;
        }
        fs::rename(&tmp, &self.path).map_err(|e| {
            Error::Storage(format!(
                "rename acl store {} -> {}: {e}",
                tmp.display(),
                self.path.display()
            ))
        })?;
        Ok(())
    }
}

/// In-memory ACL authorizer (Phase 20).
#[derive(Debug, Default)]
pub struct AclAuthorizer {
    enabled: bool,
    super_users: HashSet<String>,
    entries: Vec<AclEntry>,
}

impl AclAuthorizer {
    /// Empty authorizer (disabled).
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether enforcement is on.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Enable or disable enforcement.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Replace super-user set.
    pub fn set_super_users(&mut self, users: impl IntoIterator<Item = String>) {
        self.super_users = users.into_iter().filter(|s| !s.is_empty()).collect();
    }

    /// Current entries (clone).
    pub fn entries(&self) -> Vec<AclEntry> {
        self.entries.clone()
    }

    /// Replace entries from a snapshot (does not touch super-users).
    pub fn apply_snapshot(&mut self, snap: &AclSnapshot) {
        self.enabled = snap.enabled;
        self.entries = snap.entries.clone();
    }

    /// Snapshot for durability.
    pub fn snapshot(&self) -> AclSnapshot {
        AclSnapshot {
            enabled: self.enabled,
            entries: self.entries.clone(),
        }
    }

    /// Load entries from a JSON file and enable enforcement.
    ///
    /// Accepts a bare array (Phase 20) or [`AclSnapshot`] object.
    pub fn load_file(&mut self, path: &Path) -> Result<()> {
        let text = fs::read_to_string(path).map_err(|e| {
            Error::InvalidArgument(format!("read acl file {}: {e}", path.display()))
        })?;
        if text.trim_start().starts_with('[') {
            let list: Vec<AclEntry> = serde_json::from_str(&text).map_err(|e| {
                Error::InvalidArgument(format!("parse acl file {}: {e}", path.display()))
            })?;
            self.entries = list;
            self.enabled = true;
        } else {
            let snap: AclSnapshot = serde_json::from_str(&text).map_err(|e| {
                Error::InvalidArgument(format!("parse acl file {}: {e}", path.display()))
            })?;
            self.apply_snapshot(&snap);
            self.enabled = true;
        }
        Ok(())
    }

    /// Add entries (dedupe). Enables enforcement.
    pub fn create(&mut self, new_entries: Vec<AclEntry>) {
        for e in new_entries {
            if !self.entries.contains(&e) {
                self.entries.push(e);
            }
        }
        self.enabled = true;
    }

    /// Remove exact-matching entries. Returns how many were removed.
    pub fn delete(&mut self, victims: &[AclEntry]) -> usize {
        let before = self.entries.len();
        self.entries.retain(|e| !victims.contains(e));
        before - self.entries.len()
    }

    /// Filter list (empty principal / resource = any; resource_type `None` = any).
    pub fn list(
        &self,
        principal: Option<&str>,
        resource_type: Option<ResourceType>,
        resource: Option<&str>,
    ) -> Vec<AclEntry> {
        self.entries
            .iter()
            .filter(|e| {
                if let Some(p) = principal {
                    if !p.is_empty() && e.principal != p {
                        return false;
                    }
                }
                if let Some(rt) = resource_type {
                    if e.resource_type != rt {
                        return false;
                    }
                }
                if let Some(r) = resource {
                    if !r.is_empty() && e.resource != r {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect()
    }

    /// Authorize `principal` for `operation` on `resource_type`/`resource`.
    ///
    /// `principal == None` is treated as empty string (denied when enabled unless `*`).
    pub fn authorize(
        &self,
        principal: Option<&str>,
        resource_type: ResourceType,
        resource: &str,
        operation: AclOperation,
    ) -> bool {
        if !self.enabled {
            return true;
        }
        let p = principal.unwrap_or("");
        if self.super_users.contains(p) {
            return true;
        }
        let mut allowed = false;
        for e in &self.entries {
            if !e.matches(p, resource_type, resource, operation) {
                continue;
            }
            if e.permission == AclPermission::Deny {
                return false;
            }
            allowed = true;
        }
        allowed
    }
}

/// Shared ACL state on the broker.
#[derive(Debug)]
pub struct AclState {
    inner: RwLock<AclAuthorizer>,
    /// Principal assigned after successful shared-token Auth.
    auth_principal: RwLock<String>,
    /// Optional durable store (Phase 21).
    store: Option<AclStore>,
}

impl Default for AclState {
    fn default() -> Self {
        Self::new()
    }
}

impl AclState {
    /// Create with default auth principal `"token"` and no durable store.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(AclAuthorizer::new()),
            auth_principal: RwLock::new("token".into()),
            store: None,
        }
    }

    /// Open durable store under `data_dir` and load snapshot.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let store = AclStore::open(data_dir)?;
        let snap = store.load()?;
        let mut auth = AclAuthorizer::new();
        auth.apply_snapshot(&snap);
        Ok(Self {
            inner: RwLock::new(auth),
            auth_principal: RwLock::new("token".into()),
            store: Some(store),
        })
    }

    fn persist(&self, a: &AclAuthorizer) -> Result<()> {
        if let Some(store) = &self.store {
            store.save(&a.snapshot())?;
        }
        Ok(())
    }

    /// Configure authorizer from server flags.
    pub fn configure(
        &self,
        enable: bool,
        file: Option<&Path>,
        super_users: Vec<String>,
        auth_principal: String,
    ) -> Result<()> {
        let mut a = self.inner.write();
        a.set_super_users(super_users);
        if let Some(path) = file {
            a.load_file(path)?;
            self.persist(&a)?;
        } else if enable {
            a.set_enabled(true);
            self.persist(&a)?;
        }
        *self.auth_principal.write() = if auth_principal.is_empty() {
            "token".into()
        } else {
            auth_principal
        };
        Ok(())
    }

    /// Principal name used after token Auth.
    pub fn auth_principal(&self) -> String {
        self.auth_principal.read().clone()
    }

    /// Whether ACLs are enforced.
    pub fn is_enabled(&self) -> bool {
        self.inner.read().is_enabled()
    }

    /// Authorize a principal.
    pub fn authorize(
        &self,
        principal: Option<&str>,
        resource_type: ResourceType,
        resource: &str,
        operation: AclOperation,
    ) -> bool {
        self.inner
            .read()
            .authorize(principal, resource_type, resource, operation)
    }

    /// Create ACL entries and persist.
    pub fn create(&self, entries: Vec<AclEntry>) -> Result<()> {
        let mut a = self.inner.write();
        a.create(entries);
        self.persist(&a)
    }

    /// Delete exact entries and persist. Returns how many were removed.
    pub fn delete(&self, entries: &[AclEntry]) -> Result<usize> {
        let mut a = self.inner.write();
        let n = a.delete(entries);
        self.persist(&a)?;
        Ok(n)
    }

    /// List with optional filters.
    pub fn list(
        &self,
        principal: Option<&str>,
        resource_type: Option<ResourceType>,
        resource: Option<&str>,
    ) -> Vec<AclEntry> {
        self.inner.read().list(principal, resource_type, resource)
    }

    /// Current durable snapshot (enabled flag + entries). Super-users are not
    /// included (process-local flags only).
    pub fn snapshot(&self) -> AclSnapshot {
        self.inner.read().snapshot()
    }

    /// Replace entries + enabled from a snapshot and persist (Phase 113).
    ///
    /// Does not change super-users or auth principal.
    pub fn install_snapshot(&self, snap: &AclSnapshot) -> Result<()> {
        let mut a = self.inner.write();
        a.apply_snapshot(snap);
        self.persist(&a)
    }

    /// JSON-encode the current snapshot for inter-broker wire (Phase 113).
    pub fn encode_snapshot_bytes(&self) -> Result<Vec<u8>> {
        let snap = self.snapshot();
        serde_json::to_vec(&snap)
            .map_err(|e| Error::Storage(format!("encode acl snapshot: {e}")))
    }

    /// Decode a snapshot from inter-broker wire bytes (Phase 113).
    pub fn decode_snapshot_bytes(bytes: &[u8]) -> Result<AclSnapshot> {
        if bytes.is_empty() {
            return Ok(AclSnapshot::default());
        }
        serde_json::from_slice(bytes)
            .map_err(|e| Error::Storage(format!("decode acl snapshot: {e}")))
    }

    /// Durable store path if configured.
    pub fn store_path(&self) -> Option<PathBuf> {
        self.store.as_ref().map(|s| s.path().to_path_buf())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(
        principal: &str,
        rt: ResourceType,
        resource: &str,
        op: AclOperation,
        perm: AclPermission,
    ) -> AclEntry {
        AclEntry {
            principal: principal.into(),
            resource_type: rt,
            resource: resource.into(),
            operation: op,
            permission: perm,
        }
    }

    #[test]
    fn disabled_allows_all() {
        let a = AclAuthorizer::new();
        assert!(a.authorize(None, ResourceType::Topic, "t", AclOperation::Write));
    }

    #[test]
    fn default_deny_when_enabled() {
        let mut a = AclAuthorizer::new();
        a.set_enabled(true);
        assert!(!a.authorize(Some("alice"), ResourceType::Topic, "t", AclOperation::Write));
    }

    #[test]
    fn allow_and_deny_precedence() {
        let mut a = AclAuthorizer::new();
        a.create(vec![
            entry(
                "alice",
                ResourceType::Topic,
                "t",
                AclOperation::Write,
                AclPermission::Allow,
            ),
            entry(
                "alice",
                ResourceType::Topic,
                "t",
                AclOperation::Write,
                AclPermission::Deny,
            ),
        ]);
        assert!(!a.authorize(Some("alice"), ResourceType::Topic, "t", AclOperation::Write));
    }

    #[test]
    fn wildcard_and_super_user() {
        let mut a = AclAuthorizer::new();
        a.set_super_users(vec!["root".into()]);
        a.create(vec![entry(
            "*",
            ResourceType::Topic,
            "*",
            AclOperation::Read,
            AclPermission::Allow,
        )]);
        assert!(a.authorize(Some("bob"), ResourceType::Topic, "x", AclOperation::Read));
        assert!(!a.authorize(Some("bob"), ResourceType::Topic, "x", AclOperation::Write));
        assert!(a.authorize(Some("root"), ResourceType::Topic, "x", AclOperation::Write));
    }

    #[test]
    fn create_delete_list() {
        let mut a = AclAuthorizer::new();
        let e = entry(
            "a",
            ResourceType::Group,
            "g",
            AclOperation::Read,
            AclPermission::Allow,
        );
        a.create(vec![e.clone()]);
        assert_eq!(a.list(Some("a"), None, None).len(), 1);
        assert_eq!(a.delete(&[e]), 1);
        assert!(a.list(None, None, None).is_empty());
    }

    #[test]
    fn durable_roundtrip() {
        let dir = std::env::temp_dir().join(format!(
            "volant-acl-store-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let state = AclState::open(&dir).unwrap();
        state
            .create(vec![entry(
                "alice",
                ResourceType::Topic,
                "t",
                AclOperation::Write,
                AclPermission::Allow,
            )])
            .unwrap();
        drop(state);
        let reloaded = AclState::open(&dir).unwrap();
        assert!(reloaded.is_enabled());
        assert_eq!(reloaded.list(None, None, None).len(), 1);
        assert!(reloaded.authorize(
            Some("alice"),
            ResourceType::Topic,
            "t",
            AclOperation::Write
        ));
        let _ = fs::remove_dir_all(&dir);
    }
}

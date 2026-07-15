//! Connection stream abstraction (plain TCP or optional TLS).

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use volant_core::{Error, Result};

use crate::config::ClientConfig;

/// Broker connection used by [`crate::Client`].
pub(crate) enum ClientConn {
    /// Plaintext TCP.
    Plain(TcpStream),
    /// TLS over TCP (feature `tls`).
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl std::fmt::Debug for ClientConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain(_) => f.write_str("ClientConn::Plain"),
            #[cfg(feature = "tls")]
            Self::Tls(_) => f.write_str("ClientConn::Tls"),
        }
    }
}

impl ClientConn {
    /// Open a connection to `host:port` using `config` TLS settings.
    pub(crate) async fn connect(addr: &str, config: &ClientConfig) -> Result<Self> {
        if config.tls {
            #[cfg(feature = "tls")]
            {
                return connect_tls(addr, config).await;
            }
            #[cfg(not(feature = "tls"))]
            {
                return Err(Error::InvalidArgument(
                    "ClientConfig.tls=true requires building volant-client with `--features tls`"
                        .into(),
                ));
            }
        }
        let stream = TcpStream::connect(addr).await?;
        Ok(Self::Plain(stream))
    }
}

#[cfg(feature = "tls")]
async fn connect_tls(addr: &str, config: &ClientConfig) -> Result<ClientConn> {
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;

    use rustls::ClientConfig as RustlsClientConfig;
    use rustls::pki_types::ServerName;
    use rustls::RootCertStore;
    use tokio_rustls::TlsConnector;

    let _ = rustls::crypto::ring::default_provider().install_default();

    let tcp = TcpStream::connect(addr).await?;
    let host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(addr);

    let builder = if config.tls_insecure {
        RustlsClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoCertVerifier))
    } else {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        if let Some(ca_path) = &config.tls_ca {
            let file = File::open(ca_path).map_err(|e| {
                Error::InvalidArgument(format!(
                    "open tls_ca {}: {e}",
                    ca_path.display()
                ))
            })?;
            let mut reader = BufReader::new(file);
            let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| Error::InvalidArgument(format!("parse tls_ca PEM: {e}")))?;
            for cert in certs {
                roots
                    .add(cert)
                    .map_err(|e| Error::InvalidArgument(format!("add tls_ca cert: {e}")))?;
            }
        }
        RustlsClientConfig::builder().with_root_certificates(roots)
    };

    // Phase 19: optional client certificate for mTLS.
    let rustls_config = match (&config.tls_cert, &config.tls_key) {
        (Some(cert_path), Some(key_path)) => {
            let certs = load_client_certs(cert_path)?;
            let key = load_client_key(key_path)?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| Error::InvalidArgument(format!("client TLS cert: {e}")))?
        }
        (None, None) => builder.with_no_client_auth(),
        _ => {
            return Err(Error::InvalidArgument(
                "tls_cert and tls_key must both be set or both unset".into(),
            ));
        }
    };

    let connector = TlsConnector::from(Arc::new(rustls_config));
    let server_name = ServerName::try_from(host.to_owned()).map_err(|e| {
        Error::InvalidArgument(format!("invalid TLS server name '{host}': {e}"))
    })?;
    let tls = connector
        .connect(server_name, tcp)
        .await
        .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e)))?;
    Ok(ClientConn::Tls(Box::new(tls)))
}

#[cfg(feature = "tls")]
fn load_client_certs(
    path: &std::path::Path,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    use std::fs::File;
    use std::io::BufReader;
    let file = File::open(path).map_err(|e| {
        Error::InvalidArgument(format!("open tls_cert {}: {e}", path.display()))
    })?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::InvalidArgument(format!("parse tls_cert PEM: {e}")))?;
    if certs.is_empty() {
        return Err(Error::InvalidArgument(format!(
            "no certificates in {}",
            path.display()
        )));
    }
    Ok(certs)
}

#[cfg(feature = "tls")]
fn load_client_key(path: &std::path::Path) -> Result<rustls::pki_types::PrivateKeyDer<'static>> {
    use std::fs::File;
    use std::io::BufReader;
    let file = File::open(path).map_err(|e| {
        Error::InvalidArgument(format!("open tls_key {}: {e}", path.display()))
    })?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| Error::InvalidArgument(format!("parse tls_key PEM: {e}")))?
        .ok_or_else(|| Error::InvalidArgument(format!("no private key in {}", path.display())))
}

#[cfg(feature = "tls")]
#[derive(Debug)]
struct NoCertVerifier;

#[cfg(feature = "tls")]
impl rustls::client::danger::ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

impl AsyncRead for ClientConn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            Self::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ClientConn {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            Self::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls")]
            Self::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            Self::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

use std::io::BufReader;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

#[derive(Debug, thiserror::Error)]
pub enum TlsCertError {
    #[error("Failed to generate self-signed certificate: {0}")]
    Generation(#[source] rcgen::Error),

    #[error("Failed to read certificate file {path}: {source}")]
    CertFileRead { path: String, source: std::io::Error },

    #[error("Failed to read key file {path}: {source}")]
    KeyFileRead { path: String, source: std::io::Error },

    #[error("No certificates found in PEM file: {0}")]
    NoCertsFound(String),

    #[error("No private key found in PEM file: {0}")]
    NoKeyFound(String),

    #[error("TLS config build error: {0}")]
    RustlsError(#[from] rustls::Error),
}

/// A loaded certificate chain + private key, ready for rustls configuration.
pub struct TlsIdentity {
    pub cert_chain: Vec<CertificateDer<'static>>,
    pub private_key: PrivateKeyDer<'static>,
}

/// Generate a self-signed certificate for the given SANs using rcgen.
pub fn generate_self_signed(subject_alt_names: Vec<String>) -> Result<TlsIdentity, TlsCertError> {
    let rcgen::CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(subject_alt_names)
            .map_err(TlsCertError::Generation)?;

    let cert_der = CertificateDer::from(cert.der().clone());
    let key_der = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(key_pair.serialize_der()));

    Ok(TlsIdentity {
        cert_chain: vec![cert_der],
        private_key: key_der,
    })
}

/// Load certificate chain and private key from PEM files.
pub fn load_pem_files(
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
) -> Result<TlsIdentity, TlsCertError> {
    let cert_file = std::fs::File::open(cert_path).map_err(|e| TlsCertError::CertFileRead {
        path: cert_path.display().to_string(),
        source: e,
    })?;
    let mut cert_reader = BufReader::new(cert_file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsCertError::CertFileRead {
            path: cert_path.display().to_string(),
            source: e,
        })?;
    if certs.is_empty() {
        return Err(TlsCertError::NoCertsFound(cert_path.display().to_string()));
    }

    let key_file = std::fs::File::open(key_path).map_err(|e| TlsCertError::KeyFileRead {
        path: key_path.display().to_string(),
        source: e,
    })?;
    let mut key_reader = BufReader::new(key_file);
    let key = rustls_pemfile::pkcs8_private_keys(&mut key_reader)
        .next()
        .ok_or_else(|| TlsCertError::NoKeyFound(key_path.display().to_string()))?
        .map_err(|e| TlsCertError::KeyFileRead {
            path: key_path.display().to_string(),
            source: e,
        })?;

    Ok(TlsIdentity {
        cert_chain: certs,
        private_key: PrivateKeyDer::from(key),
    })
}

/// Load a TlsIdentity from a CertSource.
pub fn load_identity(source: &super::CertSource) -> Result<TlsIdentity, TlsCertError> {
    match source {
        super::CertSource::SelfSigned(sans) => generate_self_signed(sans.clone()),
        super::CertSource::PemFiles { cert_chain, private_key } =>
            load_pem_files(cert_chain, private_key),
    }
}

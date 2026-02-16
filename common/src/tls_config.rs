use std::sync::Arc;

use rustls::crypto::aws_lc_rs;

use super::{PqProtocol, TlsOptions};
use super::tls_cert::{self, TlsCertError};

/// Build a rustls CryptoProvider with the selected PQ key exchange groups.
pub fn crypto_provider(protocol: PqProtocol) -> rustls::crypto::CryptoProvider {
    let kx_groups: Vec<&'static dyn rustls::crypto::SupportedKxGroup> = match protocol {
        PqProtocol::X25519Mlkem768 => vec![
            aws_lc_rs::kx_group::X25519MLKEM768,
            aws_lc_rs::kx_group::X25519,
        ],
        PqProtocol::Mlkem768 => vec![
            aws_lc_rs::kx_group::MLKEM768,
            aws_lc_rs::kx_group::X25519,
        ],
        PqProtocol::X25519 => vec![
            aws_lc_rs::kx_group::X25519,
        ],
    };

    rustls::crypto::CryptoProvider {
        kx_groups,
        ..aws_lc_rs::default_provider()
    }
}

/// Build a rustls ClientConfig for TLS sender crates.
pub fn build_client_config(options: &TlsOptions) -> Result<Arc<rustls::ClientConfig>, TlsCertError> {
    let provider = Arc::new(crypto_provider(options.pq_protocol));

    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(TlsCertError::RustlsError)?;

    let mut config = if options.danger_skip_verify {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier))
            .with_no_client_auth()
    } else {
        let identity = tls_cert::load_identity(&options.cert_source)?;
        let mut root_store = rustls::RootCertStore::empty();
        for cert in &identity.cert_chain {
            root_store.add(cert.clone()).map_err(|e| {
                TlsCertError::RustlsError(rustls::Error::General(format!("{}", e)))
            })?;
        }
        builder
            .with_root_certificates(root_store)
            .with_no_client_auth()
    };

    if let Some(size) = options.max_fragment_size {
        config.max_fragment_size = Some(size);
    }

    Ok(Arc::new(config))
}

/// Build a rustls ServerConfig for TLS receiver crates.
pub fn build_server_config(options: &TlsOptions) -> Result<Arc<rustls::ServerConfig>, TlsCertError> {
    let provider = Arc::new(crypto_provider(options.pq_protocol));
    let identity = tls_cert::load_identity(&options.cert_source)?;

    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(TlsCertError::RustlsError)?
        .with_no_client_auth()
        .with_single_cert(identity.cert_chain, identity.private_key)
        .map_err(TlsCertError::RustlsError)?;

    if let Some(size) = options.max_fragment_size {
        config.max_fragment_size = Some(size);
    }

    Ok(Arc::new(config))
}

/// Certificate verifier that accepts all certificates (DANGER: testing only).
#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

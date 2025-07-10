use rustls::pki_types::pem::PemObject;
use rustls::pki_types::CertificateDer;
use rustls::{ClientConfig, RootCertStore};

use std::env;
use std::path::{Path, PathBuf};

fn load_root_cert(path: &Path) -> RootCertStore {
    let cert = CertificateDer::from_pem_file(path).expect("Failed to load certificate");
    let mut certs = RootCertStore::empty();
    certs
        .add(cert)
        .expect("Failed to add certificate to root store");
    certs
}

pub fn configure_client() -> ClientConfig {
    // Usar CARGO_MANIFEST_DIR para resolver la ruta al archivo `cert.crt`
    let cert_path: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..") // Retroceder un nivel desde el crate actual
        .join("certs") // Carpeta donde está el certificado
        .join("cert.crt");

    let root_store = load_root_cert(&cert_path);

    // Configurar el proveedor criptográfico
    match rustls::crypto::aws_lc_rs::default_provider().install_default() {
        Ok(_) => {}
        Err(err) => {
            eprintln!("Failed to install CryptoProvider: {:?}", err);
        }
    }

    ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth()
}

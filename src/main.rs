use anyhow::{anyhow, Result};

use memflux::config::Config;
use memflux::protocol::parse_command_from_stream;
use memflux::types::Response;
use memflux::MemFluxDB;

use std::fs::File;
use std::io::BufReader as StdBufReader;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

fn generate_self_signed_cert(cert_path: &str, key_path: &str) -> Result<()> {
    println!("Generating self-signed certificate...");
    let subject_alt_names = vec!["localhost".to_string()];
    let cert = rcgen::generate_simple_self_signed(subject_alt_names)?;
    std::fs::write(cert_path, cert.serialize_pem()?)?;
    std::fs::write(key_path, cert.serialize_private_key_pem())?;
    println!("Certificate created at: {}", cert_path);
    println!("Key created at: {}", key_path);
    Ok(())
}

fn load_tls_config(config: &Config) -> Result<Arc<rustls::ServerConfig>> {
    let cert_path = &config.cert_file;
    let key_path = &config.key_file;

    if !Path::new(cert_path).exists() || !Path::new(key_path).exists() {
        generate_self_signed_cert(cert_path, key_path)?;
    }

    let certs = {
        let cert_file = File::open(cert_path)?;
        let mut cert_reader = StdBufReader::new(cert_file);
        rustls_pemfile::certs(&mut cert_reader)?
            .into_iter()
            .map(rustls::Certificate)
            .collect()
    };

    let key = {
        let key_file = File::open(key_path)?;
        let mut key_reader = StdBufReader::new(key_file);
        rustls_pemfile::pkcs8_private_keys(&mut key_reader)?
            .into_iter()
            .map(rustls::PrivateKey)
            .next()
            .ok_or_else(|| anyhow!("No private key found in {}", key_path))?
    };

    let tls_config = rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    Ok(Arc::new(tls_config))
}

#[tokio::main]
async fn main() -> Result<()> {
    let db = Arc::new(MemFluxDB::open("config.json").await?);
    let config = db.app_context.config.clone();

    let addr = format!("{}:{}", config.host, config.port);
    let listener = TcpListener::bind(&addr).await?;

    let acceptor: Option<TlsAcceptor> = if config.encrypt {
        println!("TLS encryption is enabled.");
        let tls_config = load_tls_config(&config)?;
        Some(TlsAcceptor::from(tls_config))
    } else {
        None
    };

    println!("MemFlux (RESP Protocol) listening on {}", addr);
    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let db_clone = db.clone();
        let acceptor_clone = acceptor.clone();

        let task = async move {
            if let Some(acceptor) = acceptor_clone {
                match acceptor.accept(stream).await {
                    Ok(tls_stream) => {
                        if let Err(e) = handle_connection(tls_stream, db_clone).await {
                            if e.downcast_ref::<std::io::Error>().map_or(true, |io_err| {
                                io_err.kind() != std::io::ErrorKind::BrokenPipe
                            }) {
                                eprintln!("Connection error from {}: {:?}", peer_addr, e);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("TLS handshake error from {}: {}", peer_addr, e);
                    }
                }
            } else {
                if let Err(e) = handle_connection(stream, db_clone).await {
                    if e.downcast_ref::<std::io::Error>().map_or(true, |io_err| {
                        io_err.kind() != std::io::ErrorKind::BrokenPipe
                    }) {
                        eprintln!("Connection error from {}: {:?}", peer_addr, e);
                    }
                }
            }
        };
        tokio::spawn(task);
    }
}

async fn handle_connection<S>(stream: S, db: Arc<MemFluxDB>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut buf_reader = BufReader::new(reader);
    let mut authenticated = db.app_context.config.requirepass.is_empty();

    loop {
        match parse_command_from_stream(&mut buf_reader).await {
            Ok(Some(command)) => {
                if !authenticated {
                    if command.name == "AUTH" {
                        if command.args.len() == 2
                            && String::from_utf8_lossy(&command.args[1])
                                == db.app_context.config.requirepass
                        {
                            authenticated = true;
                            writer.write_all(&Response::Ok.into_protocol_format()).await?;
                        } else {
                            let response = Response::Error("Invalid password".to_string());
                            writer.write_all(&response.into_protocol_format()).await?;
                        }
                    } else {
                        let response =
                            Response::Error("NOAUTH Authentication required.".to_string());
                        writer.write_all(&response.into_protocol_format()).await?;
                    }
                    continue;
                }

                let response = db.execute_command(command).await;
                writer.write_all(&response.into_protocol_format()).await?;
            }
            Ok(None) => {
                println!("Client disconnected.");
                break;
            }
            Err(e) => {
                let response = Response::Error(e.to_string());
                let _ = writer.write_all(&response.into_protocol_format()).await;
                if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
                    if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}
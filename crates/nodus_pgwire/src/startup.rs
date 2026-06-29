//! Startup/authentication handler: SASL/SCRAM-SHA-256 auth against the
//! `PasswordAuthenticator`, server-parameter negotiation, and backend key data.
//!
//! Only `SCRAM-SHA-256` is offered, so the password never crosses the wire — the
//! client proves knowledge of it through a challenge/response, and the server
//! checks the proof against the stored verifier material. The in-progress
//! exchange for a connection is kept in `scram_states`, keyed by session id, and
//! removed when the handshake finishes, fails, or the connection drops.

use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Sink, SinkExt};
use nodus_catalog::PrincipalId;
use nodus_security::{PasswordAuthenticator, ScramVerifier, Session, SessionRegistry};
use pgwire::api::auth::{
    DefaultServerParameterProvider, LoginInfo, ServerParameterProvider, StartupHandler,
    save_startup_parameters_to_metadata,
};
use pgwire::api::{ClientInfo, PgWireConnectionState};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::response::{ErrorResponse, ReadyForQuery};
use pgwire::messages::startup::{Authentication, BackendKeyData, ParameterStatus, SecretKey};

use crate::client_meta::*;
use crate::server::ConnState;
use crate::{
    METADATA_BACKEND_PID, METADATA_BACKEND_SECRET, METADATA_NODUS_PRINCIPAL_ID,
    METADATA_NODUS_SESSION_ID, METADATA_TX_STATUS,
};

const SCRAM_SHA_256: &str = "SCRAM-SHA-256";

/// The state of an in-progress SCRAM exchange between the two client messages.
struct ScramExchange {
    username: String,
    verifier: ScramVerifier,
}

pub struct NodusStartupHandler {
    authenticator: Arc<PasswordAuthenticator>,
    param_provider: Arc<DefaultServerParameterProvider>,
    registry: Arc<SessionRegistry>,
    /// This connection's session state, shared with the server's post-loop
    /// cleanup so it can release the session once the connection ends.
    conn: Arc<ConnState>,
    /// This connection's in-flight SCRAM exchange — present only between the
    /// client's first and final SASL messages. Per-connection, so it is dropped
    /// automatically when the connection (and this handler) goes away.
    scram: Mutex<Option<ScramExchange>>,
}

impl NodusStartupHandler {
    pub(crate) fn new(
        authenticator: Arc<PasswordAuthenticator>,
        param_provider: Arc<DefaultServerParameterProvider>,
        registry: Arc<SessionRegistry>,
        conn: Arc<ConnState>,
    ) -> Self {
        Self {
            authenticator,
            param_provider,
            registry,
            conn,
            scram: Mutex::new(None),
        }
    }

    /// Registers this connection's session and backend key, stamps the session
    /// metadata onto the client, and records the session id for the server's
    /// post-loop cleanup. Runs once, on the first (Startup) message.
    fn register_session<C: ClientInfo>(&self, client: &mut C) {
        let session_id = uuid::Uuid::new_v4().to_string();
        let secret = (uuid::Uuid::new_v4().as_u128() & 0x7fff_ffff) as i32;
        let pid = std::process::id() as i32;
        let session = Session {
            session_id: session_id.clone(),
            principal_id: PrincipalId::new(),
            active_roles: vec![],
            database_id: None,
        };
        self.registry.register(&session);
        self.registry.register_backend_key(&session_id, pid, secret);
        let metadata = client.metadata_mut();
        metadata.insert(METADATA_NODUS_SESSION_ID.to_owned(), session_id.clone());
        metadata.insert(METADATA_BACKEND_PID.to_owned(), pid.to_string());
        metadata.insert(METADATA_BACKEND_SECRET.to_owned(), secret.to_string());
        metadata.insert(METADATA_TX_STATUS.to_owned(), "I".to_owned());
        *self.conn.session_id.lock().unwrap() = Some(session_id);
    }
}

async fn finish_nodus_authentication<C>(
    client: &mut C,
    server_parameter_provider: &DefaultServerParameterProvider,
) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    client
        .send(PgWireBackendMessage::Authentication(Authentication::Ok))
        .await?;

    if let Some(mut parameters) = server_parameter_provider.server_parameters(client) {
        parameters.insert("server_version_num".to_owned(), "180000".to_owned());
        parameters.insert("TimeZone".to_owned(), "UTC".to_owned());
        parameters.insert("IntervalStyle".to_owned(), "postgres".to_owned());
        parameters.insert("standard_conforming_strings".to_owned(), "on".to_owned());
        parameters.insert("is_superuser".to_owned(), "on".to_owned());
        parameters.insert("session_authorization".to_owned(), "nodus".to_owned());
        let app = client
            .metadata()
            .get("application_name")
            .cloned()
            .unwrap_or_default();
        parameters.insert("application_name".to_owned(), app);
        let mut parameters: Vec<_> = parameters.into_iter().collect();
        parameters.sort_by(|a, b| a.0.cmp(&b.0));
        for (name, value) in parameters {
            client
                .send(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                    name, value,
                )))
                .await?;
        }
    }

    let pid = client
        .metadata()
        .get(METADATA_BACKEND_PID)
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(std::process::id() as i32);
    let secret = client
        .metadata()
        .get(METADATA_BACKEND_SECRET)
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or_default();
    client
        .send(PgWireBackendMessage::BackendKeyData(BackendKeyData::new(
            pid,
            SecretKey::I32(secret),
        )))
        .await?;
    client
        .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
            tx_status_from_client(client),
        )))
        .await?;
    client.flush().await?;
    client.set_state(PgWireConnectionState::ReadyForQuery);
    Ok(())
}

/// Sends a fatal `28P01` (invalid authorization) and closes the connection. Used
/// for every SCRAM failure mode so the client cannot distinguish an unknown user
/// from a bad password or a malformed message.
async fn reject_authentication<C>(client: &mut C) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let error_info = ErrorInfo::new(
        "FATAL".to_owned(),
        "28P01".to_owned(),
        "password authentication failed".to_owned(),
    );
    client
        .feed(PgWireBackendMessage::ErrorResponse(ErrorResponse::from(
            error_info,
        )))
        .await?;
    client.close().await?;
    Ok(())
}

#[async_trait]
impl StartupHandler for NodusStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: pgwire::messages::PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            pgwire::messages::PgWireFrontendMessage::Startup(ref startup) => {
                // The first message of the connection: register the session here
                // (pgwire's `process_socket` owns the loop, so there is no earlier
                // hook), then begin authentication.
                self.register_session(client);
                save_startup_parameters_to_metadata(client, startup);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireBackendMessage::Authentication(Authentication::SASL(
                        vec![SCRAM_SHA_256.to_owned()],
                    )))
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::PasswordMessageFamily(msg) => {
                let session_id = session_id_from_client(client);
                let mid_exchange = self.scram.lock().unwrap().is_some();

                if !mid_exchange {
                    // Step 1: client-first (SASLInitialResponse). Select the
                    // mechanism, parse the client-first message, look up the
                    // user's verifier, and answer with server-first.
                    let sasl = msg.into_sasl_initial_response()?;
                    if sasl.auth_method != SCRAM_SHA_256 {
                        return reject_authentication(client).await;
                    }
                    let data = sasl.data.unwrap_or_default();
                    let cf = match nodus_security::ClientFirst::parse(&data) {
                        Ok(cf) => cf,
                        Err(_) => return reject_authentication(client).await,
                    };
                    // PostgreSQL carries the username in the startup `user`
                    // parameter, not in the SCRAM `n=` field (which clients leave
                    // empty), so resolve the credential from the login info.
                    let username = LoginInfo::from_client_info(client)
                        .user()
                        .map(|u| u.to_string())
                        .unwrap_or_default();
                    let Some(keys) = self.authenticator.scram_keys(&username) else {
                        return reject_authentication(client).await;
                    };
                    let server_nonce = uuid::Uuid::new_v4().simple().to_string();
                    let (server_first, verifier) = ScramVerifier::start(&cf, &keys, &server_nonce);
                    *self.scram.lock().unwrap() = Some(ScramExchange { username, verifier });
                    client
                        .send(PgWireBackendMessage::Authentication(
                            Authentication::SASLContinue(Bytes::from(server_first.into_bytes())),
                        ))
                        .await?;
                } else {
                    // Step 2: client-final (SASLResponse). Verify the proof, send
                    // server-final, then complete the startup.
                    let sasl = msg.into_sasl_response()?;
                    let exchange = self.scram.lock().unwrap().take();
                    let Some(exchange) = exchange else {
                        return reject_authentication(client).await;
                    };
                    let server_final = match exchange.verifier.finish(&sasl.data) {
                        Ok(msg) => msg,
                        Err(_) => return reject_authentication(client).await,
                    };
                    client
                        .send(PgWireBackendMessage::Authentication(
                            Authentication::SASLFinal(Bytes::from(server_final.into_bytes())),
                        ))
                        .await?;
                    match self.authenticator.issue_session(&exchange.username) {
                        Ok(session) => {
                            self.registry
                                .update_principal(&session_id, session.principal_id);
                            client.metadata_mut().insert(
                                METADATA_NODUS_PRINCIPAL_ID.to_string(),
                                session.principal_id.to_string(),
                            );
                            finish_nodus_authentication(client, &self.param_provider).await?;
                        }
                        Err(_) => return reject_authentication(client).await,
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

//! Startup/authentication handler: cleartext password auth against the
//! `PasswordAuthenticator`, server-parameter negotiation, and backend key data.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{Sink, SinkExt};
use nodus_security::{Authenticator, PasswordAuthenticator, SessionRegistry};
use pgwire::api::auth::{
    DefaultServerParameterProvider, LoginInfo, ServerParameterProvider, StartupHandler,
    save_startup_parameters_to_metadata,
};
use pgwire::api::{ClientInfo, PgWireConnectionState};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::response::{ErrorResponse, ReadyForQuery};
use pgwire::messages::startup::{Authentication, BackendKeyData, ParameterStatus};

use crate::client_meta::*;
use crate::{METADATA_BACKEND_PID, METADATA_BACKEND_SECRET, METADATA_NODUS_PRINCIPAL_ID};

pub struct NodusStartupHandler {
    pub(crate) authenticator: Arc<PasswordAuthenticator>,
    pub(crate) param_provider: DefaultServerParameterProvider,
    pub(crate) registry: Arc<SessionRegistry>,
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
            pid, secret,
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
                save_startup_parameters_to_metadata(client, startup);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::CleartextPassword,
                    ))
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                let pwd = pwd.into_password()?;
                let login = LoginInfo::from_client_info(client);
                let user = login.user().map(|u| u.to_string()).unwrap_or_default();
                match self.authenticator.authenticate(&user, &pwd.password) {
                    Ok(session) => {
                        let session_id = session_id_from_client(client);
                        self.registry
                            .update_principal(&session_id, session.principal_id);
                        client.metadata_mut().insert(
                            METADATA_NODUS_PRINCIPAL_ID.to_string(),
                            session.principal_id.to_string(),
                        );
                        finish_nodus_authentication(client, &self.param_provider).await?;
                    }
                    Err(_) => {
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
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

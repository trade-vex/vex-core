use std::{ffi::CString, fmt::Display};
use tokio::sync::{mpsc::Receiver, oneshot};
use rusteron_client::{Aeron, AeronCError, AeronContext, AeronPublication, AeronSubscription};
use tracing::{error, info};
use crate::{server::{config::CoreConfig, duologue::{DuologueImageAvailable, DuologueImageUnavailable}, server::{GatewayImageAvailableHandler, GatewayImageUnavailableHandler}, ServerError}, utils::{new_publication_with_channel, new_subscription_with_channel}};

/// Centralized Aeron Actor that handles all Aeron operations
pub struct AeronActor {
    aeron: Aeron,
}
pub enum AeronCommand {
    CreatePublication {
        channel: CString,
        stream_id: i32,
        reply: oneshot::Sender<Result<AeronPublication, AeronCError>>,
    },
    CreateAllGatewaySubscription {
        channel: CString,
        stream_id: i32,
        reply: oneshot::Sender<Result<AeronSubscription, AeronCError>>,
        image_available_handler: GatewayImageAvailableHandler,
        image_unavailable_handler: GatewayImageUnavailableHandler,
    },
    CreateGatewaySubscription {
        channel: CString,
        stream_id: i32,
        reply: oneshot::Sender<Result<AeronSubscription, AeronCError>>,
        image_available_handler: DuologueImageAvailable,
        image_unavailable_handler: DuologueImageUnavailable,
    },
    Shutdown,   
}

impl Display for AeronCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AeronCommand::CreatePublication { channel, stream_id, .. } => {
                write!(f, "CreatePublication {{ channel: {:?}, stream_id: {:?} }}", channel, stream_id)
            }
            AeronCommand::CreateAllGatewaySubscription { channel, stream_id, .. } => {
                write!(f, "CreateAllGatewaySubscription {{ channel: {:?}, stream_id: {:?} }}", channel, stream_id)
            }
            AeronCommand::CreateGatewaySubscription { channel, stream_id, .. } => {
                write!(f, "CreateGatewaySubscription {{ channel: {:?}, stream_id: {:?} }}", channel, stream_id)
            }
            AeronCommand::Shutdown => {
                write!(f, "Shutdown")
            }
        }
    }
}

impl AeronActor {
    pub fn new(config: CoreConfig) -> Result<Self, ServerError> {
        // Initialize Aeron context
        let ctx = AeronContext::new()?;
        let context_dir = std::ffi::CString::new(config.context_dir.clone())?;
        info!("AeronActor context_dir: {:?}", context_dir);
        ctx.set_dir(&context_dir)?;
        ctx.set_driver_timeout_ms(5_000)?;

        // Create Aeron instance
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;
        Ok(Self {
            aeron,
        })
    }

    pub async fn run(&self, mut rx: Receiver<AeronCommand>) -> Result<(), ServerError> {
        info!("AeronActor started");

        while let Some(command) = rx.recv().await {
            info!("AeronActor received command: {}", command);
            match command {
                AeronCommand::CreatePublication { channel, stream_id, reply } => {
                    let result = new_publication_with_channel(&self.aeron, &channel, stream_id);
                    let x = reply.send(result);
                    match x {
                        Ok(()) => {
                            info!("AeronActor sent reply for command create publication");
                        }
                        Err(e) => {
                            error!("could not send reply for command create publication: {:?}", e);
                        }
                    }
                }
                AeronCommand::CreateAllGatewaySubscription { channel, stream_id, image_available_handler, image_unavailable_handler, reply } => {
                    let result = new_subscription_with_channel(&self.aeron, &channel, stream_id, image_available_handler, image_unavailable_handler);
                    let _ = reply.send(result);
                    info!("AeronActor sent reply for command create all gateway subscription");
                }
                AeronCommand::CreateGatewaySubscription { channel, stream_id, image_available_handler, image_unavailable_handler, reply } => {
                    let result = new_subscription_with_channel(&self.aeron, &channel, stream_id, image_available_handler, image_unavailable_handler);
                    let _ = reply.send(result);
                    info!("AeronActor sent reply for command create gateway subscription");
                }
                AeronCommand::Shutdown => {
                    info!("AeronActor shutting down");
                    self.aeron.close()?;
                    break;
                }
            }
        }

        Ok(())
    }
}


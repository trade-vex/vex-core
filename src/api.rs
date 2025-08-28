use common::cmd::OrderCommand;
use tokio::sync::mpsc;

#[derive(Clone)]
// ExchangeApi is the interface through which external clients can interact with the exchange core.
// (later it will be a client gateway sending events on to the ringbuffer disruptor and from there to the risk engine, matching engine, etc.)
pub struct ExchangeApi {
    command_tx: mpsc::Sender<OrderCommand>,
}

impl ExchangeApi {
    // Creates a new instance of ExchangeApi with the provided command sender.
    pub fn new(command_tx: mpsc::Sender<OrderCommand>) -> Self {
        Self { command_tx }
    }
    // Submits a command to the exchange core.
    pub async fn submit_command(&self, cmd: OrderCommand) -> Result<(), &'static str> {
        self.command_tx
            .send(cmd)
            .await
            .map_err(|_| "Failed to send command to core")
    }
}

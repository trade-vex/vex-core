//! Test client wrapper around VexGateway
//!
//! This module provides a convenient wrapper around the VexGateway client
//! for sending OrderCommands and receiving responses in tests.

use crate::test_framework::types::*;
use common::OrderCommand;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};
use tracing::{debug, error, info};
use vex_config::GatewayNetworkingConfig;
use vex_networking::client::{OrderCommandHandler, Publisher, VexGateway};

/// How long the core gets to accept a gateway after a start: the topic check, after a `kill -9`
/// the wait for the media driver to release the dead core's journal (up to its client liveness
/// timeout, 10 s, and 5 s), the replay and its re-publication.
const START_TIMEOUT: Duration = Duration::from_secs(120);

/// Test client for interacting with VexCore
///
/// Dropping it stops the gateway's polling thread, so a test can connect again, to a restarted
/// core, without leaving the previous connection spinning.
pub struct TestClient {
    gateway: VexGateway,
    publisher: Publisher,
    receiver: Receiver<OrderCommand>,
    timeout: Duration,
    client_id: u8,
}

impl TestClient {
    /// Connect a new test client, retrying until the core accepts a gateway, which it does once
    /// it has started and replayed its journal. The arguments are [`TestClient::new`]'s.
    pub fn connect(
        client_id: u8,
        timeout: Duration,
        core_address: &str,
        core_port: u16,
        core_control_port: u16,
    ) -> TestResult<Self> {
        let start = Instant::now();
        loop {
            match Self::new(
                client_id,
                timeout,
                core_address,
                core_port,
                core_control_port,
            ) {
                Err(e) if start.elapsed() < START_TIMEOUT => {
                    info!("core not accepting gateways yet: {e}");
                    std::thread::sleep(Duration::from_secs(1));
                }
                result => return result,
            }
        }
    }

    /// Create a new test client
    ///
    /// # Arguments
    /// * `client_id` - Unique client identifier (0-255)
    /// * `timeout` - Default timeout for receive operations
    /// * `core_address` - VEX Core server address
    /// * `core_port` - VEX Core server port
    /// * `core_control_port` - VEX Core server control port
    pub fn new(
        client_id: u8,
        timeout: Duration,
        core_address: &str,
        core_port: u16,
        core_control_port: u16,
    ) -> TestResult<Self> {
        let mut config = GatewayNetworkingConfig::test_defaults();
        config.gateway_id = client_id;
        config.core_address = core_address.to_string();
        config.core_port = core_port;
        config.core_control_port = core_control_port;

        let mut gateway = VexGateway::new(config).map_err(|e| TestError::Network(e.to_string()))?;

        let (sx, rx) = mpsc::channel();
        let handler = OrderCommandHandler::new(gateway.gateway_id(), sx);
        let publisher = gateway
            .start(handler)
            .map_err(|e| TestError::Network(e.to_string()))?;

        debug!("Test client {} started successfully", client_id);

        Ok(Self {
            gateway,
            publisher,
            receiver: rx,
            timeout,
            client_id,
        })
    }

    /// Send an OrderCommand to VexCore
    pub fn send(&self, cmd: &OrderCommand) -> TestResult<()> {
        self.publisher
            .send_order_command(cmd)
            .map_err(|e| TestError::Network(e.to_string()))?;

        debug!(
            "Client {} sent command: order_id={}, command={:?}",
            self.client_id, cmd.order_id, cmd.command
        );

        Ok(())
    }

    /// Receive an OrderCommand response from VexCore
    ///
    /// Blocks until a response is received or timeout expires
    pub fn recv(&mut self) -> TestResult<OrderCommand> {
        match self.receiver.recv_timeout(self.timeout) {
            Ok(response) => {
                debug!(
                    "Client {} received response: order_id={}, status={:?}",
                    self.client_id, response.order_id, response.status
                );
                Ok(response)
            }
            Err(RecvTimeoutError::Timeout) => {
                error!(
                    "Client {} receive timeout after {:?}",
                    self.client_id, self.timeout
                );
                Err(TestError::Timeout {
                    timeout: self.timeout,
                })
            }
            Err(RecvTimeoutError::Disconnected) => {
                error!("Client {} channel disconnected", self.client_id);
                Err(TestError::Network(
                    "Client channel disconnected".to_string(),
                ))
            }
        }
    }

    /// Send command and wait for response (convenience method)
    pub fn send_and_recv(&mut self, cmd: OrderCommand) -> TestResult<OrderCommand> {
        self.send(&cmd)?;
        self.recv()
    }

    /// Drain any pending responses (useful for cleanup between tests)
    pub fn drain_responses(&mut self) -> Vec<OrderCommand> {
        let mut responses = Vec::new();
        while let Ok(response) = self.receiver.recv_timeout(Duration::from_millis(10)) {
            responses.push(response);
        }
        if !responses.is_empty() {
            debug!(
                "Client {} drained {} pending responses",
                self.client_id,
                responses.len()
            );
        }
        responses
    }
}

impl Drop for TestClient {
    fn drop(&mut self) {
        if let Err(e) = self.gateway.shutdown() {
            error!("Client {} did not shut down: {}", self.client_id, e);
        }
    }
}

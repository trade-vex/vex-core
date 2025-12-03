//! Test client wrapper around VexGateway
//!
//! This module provides a convenient wrapper around the VexGateway client
//! for sending OrderCommands and receiving responses in tests.

use crate::test_framework::types::*;
use common::OrderCommand;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::Duration;
use tracing::{debug, error};
use vex_config::GatewayNetworkingConfig;
use vex_networking::client::{OrderCommandHandler, Publisher, VexGateway};

/// Test client for interacting with VexCore
pub struct TestClient {
    publisher: Publisher,
    receiver: Receiver<OrderCommand>,
    timeout: Duration,
    client_id: u8,
}

impl TestClient {
    /// Create a new test client
    ///
    /// # Arguments
    /// * `client_id` - Unique client identifier (0-255)
    /// * `timeout` - Default timeout for receive operations
    pub fn new(client_id: u8, timeout: Duration) -> TestResult<Self> {
        let mut config = GatewayNetworkingConfig::test_defaults();
        config.gateway_id = client_id;

        let mut gateway = VexGateway::new(config).map_err(|e| TestError::Network(e.to_string()))?;

        let (sx, rx) = mpsc::channel();
        let handler = OrderCommandHandler::new(gateway.gateway_id(), sx);
        let publisher = gateway
            .start(handler)
            .map_err(|e| TestError::Network(e.to_string()))?;

        debug!("Test client {} started successfully", client_id);

        Ok(Self {
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

    /// Receive with custom timeout
    pub fn recv_timeout(&mut self, timeout: Duration) -> TestResult<OrderCommand> {
        match self.receiver.recv_timeout(timeout) {
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
                    self.client_id, timeout
                );
                Err(TestError::Timeout { timeout })
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

    /// Send command and wait for response with custom timeout
    pub fn send_and_recv_timeout(
        &mut self,
        cmd: OrderCommand,
        timeout: Duration,
    ) -> TestResult<OrderCommand> {
        self.send(&cmd)?;
        self.recv_timeout(timeout)
    }

    /// Get the client ID
    pub fn client_id(&self) -> u8 {
        self.client_id
    }

    /// Set default timeout
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
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

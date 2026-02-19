//! Test framework for VEX-CORE integration testing
//!
//! This module provides the main TestContext for orchestrating tests,
//! managing test state, and coordinating verification.

pub mod client;
pub mod redis;
pub mod types;

use self::client::TestClient;
use self::redis::RedisVerifier;
use self::types::*;
use common::OrderCommand;
use hashbrown::HashMap;
use std::time::Duration;
use tracing::{debug, info};

/// Main test context that orchestrates test execution
///
/// The TestContext manages:
/// - Test client for sending/receiving OrderCommands
/// - Redis verifier for state validation
/// - User state tracking
/// - Test configuration
///
/// # Example
/// ```no_run
/// use xtask::test_framework::TestContext;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut ctx = TestContext::new().await?;
///
/// // Fund a user
/// ctx.fund_user(1, 1000000, 1).await?;  // user_id=1, amount=1M, asset_id=1 (USD)
///
/// // Execute a test
/// // ... send orders, verify results ...
///
/// // Cleanup
/// ctx.cleanup().await?;
/// # Ok(())
/// # }
/// ```
pub struct TestContext {
    /// Test client for sending commands
    pub client: TestClient,
    /// Redis verifier for state validation
    pub redis: RedisVerifier,
    /// Market ID being tested
    pub market_id: u32,
    /// Base asset ID
    pub base_asset_id: u16,
    /// Quote asset ID
    pub quote_asset_id: u16,
    /// User state tracker
    users: HashMap<u64, UserState>,
    /// Test configuration
    config: TestConfig,
}

impl TestContext {
    /// Create a new test context with default configuration
    pub async fn new() -> TestResult<Self> {
        Self::with_config(TestConfig::default()).await
    }

    /// Create a new test context with custom configuration
    pub async fn with_config(config: TestConfig) -> TestResult<Self> {
        info!("Initializing test context");
        info!("  Market ID: {}", config.market_id);
        info!("  Base asset: {}", config.base_asset_id);
        info!("  Quote asset: {}", config.quote_asset_id);
        info!("  Redis: {}:{}", config.redis_host, config.redis_port);

        let client = TestClient::new(
            0,
            config.default_timeout,
            &config.core_address,
            config.core_port,
            config.core_control_port,
        )?;
        let redis = RedisVerifier::new(&config.redis_host, config.redis_port).await?;

        Ok(Self {
            client,
            redis,
            market_id: config.market_id,
            base_asset_id: config.base_asset_id,
            quote_asset_id: config.quote_asset_id,
            users: HashMap::new(),
            config,
        })
    }

    /// Get or create user state
    pub fn user(&mut self, user_id: u64) -> &mut UserState {
        self.users
            .entry(user_id)
            .or_insert_with(|| UserState::new(user_id))
    }

    /// Execute an OrderCommand and return the response
    ///
    /// This is the primary method for test execution.
    /// It sends the command and waits for the response.
    pub fn execute_command(&mut self, cmd: OrderCommand) -> TestResult<OrderCommand> {
        debug!(
            "Executing command: {:?} for user {}",
            cmd.command, cmd.user_id
        );
        self.client.send_and_recv(cmd)
    }

    /// Execute an OrderCommand with custom timeout
    pub fn execute_command_timeout(
        &mut self,
        cmd: OrderCommand,
        timeout: Duration,
    ) -> TestResult<OrderCommand> {
        debug!(
            "Executing command (timeout={:?}): {:?} for user {}",
            timeout, cmd.command, cmd.user_id
        );
        self.client.send_and_recv_timeout(cmd, timeout)
    }

    /// Fund a user with a specific asset (send deposit command)
    ///
    /// This is a convenience method for setting up test users.
    /// It sends a DepositFunds command and waits for confirmation.
    pub async fn fund_user(&mut self, user_id: u64, amount: u64, asset_id: u16) -> TestResult<()> {
        info!(
            "Funding user {} with {} units of asset {}",
            user_id, amount, asset_id
        );

        let deposit_cmd = OrderCommand::deposit_funds(user_id, amount, asset_id);
        let response = self.execute_command(deposit_cmd)?;

        if response.status != common::Status::Processed {
            return Err(TestError::Verification {
                message: format!(
                    "Deposit failed for user {}: status={:?}",
                    user_id, response.status
                ),
            });
        }

        // Wait for balance to appear in Redis
        let balance = self
            .redis
            .wait_for_balance_update(user_id, asset_id, self.config.redis_event_timeout)
            .await?;

        if balance.total != amount {
            return Err(TestError::Verification {
                message: format!(
                    "Balance mismatch after deposit: expected {}, got {}",
                    amount, balance.total
                ),
            });
        }

        // Update expected state
        self.user(user_id)
            .set_expected_balance(asset_id, Balance::from_total(amount));

        debug!("User {} funded successfully", user_id);
        Ok(())
    }

    /// Get configuration
    pub fn config(&self) -> &TestConfig {
        &self.config
    }

    /// Cleanup test data
    pub async fn cleanup(&mut self) -> TestResult<()> {
        info!("Cleaning up test context");

        // Drain any pending responses
        self.client.drain_responses();

        // Cleanup Redis test data
        // Clean up test users, orders, trades
        for user_id in self.users.keys() {
            let pattern = format!("user:{}", user_id);
            self.redis.cleanup_test_data(&pattern).await?;
        }

        // Clean up market data
        let market_pattern = format!("market:{}", self.market_id);
        self.redis.cleanup_test_data(&market_pattern).await?;

        let orderbook_pattern = format!("orderbook:market:{}", self.market_id);
        self.redis.cleanup_test_data(&orderbook_pattern).await?;

        debug!("Test context cleaned up successfully");
        Ok(())
    }

    /// Verify system-wide invariants
    ///
    /// This checks that the system state is consistent across all users.
    /// Should be called periodically during long-running tests.
    pub async fn verify_invariants(&mut self) -> TestResult<()> {
        debug!("Verifying system invariants");

        // Verify balance invariants for all users
        for user in self.users.values() {
            for (&asset_id, _) in &user.expected_balances {
                let balance = self.redis.get_balance(user.user_id, asset_id).await?;
                balance.verify_invariant()?;
            }
        }

        debug!("System invariants verified successfully");
        Ok(())
    }
}

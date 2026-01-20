//! Redis verification utilities for the test framework
//!
//! This module provides Redis connection management and state verification
//! functions for validating that events are correctly published to Redis.

use crate::test_framework::types::*;
use redis::{AsyncCommands, Client, aio::ConnectionManager};
use serde_json;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tracing::{debug, info, warn};

/// Redis verifier for checking state published by the Events Handler
pub struct RedisVerifier {
    #[allow(dead_code)]
    client: Client,
    conn: ConnectionManager,
}

impl RedisVerifier {
    /// Create a new Redis verifier
    pub async fn new(host: &str, port: u16) -> TestResult<Self> {
        let redis_url = format!("redis://{}:{}/", host, port);
        let client = Client::open(redis_url.as_str())?;
        let conn = ConnectionManager::new(client.clone()).await?;

        debug!("Connected to Redis at {}:{}", host, port);

        Ok(Self { client, conn })
    }

    /// Get balance for a user and asset from Redis
    /// Key format: user:{user_id}:asset:{asset_id}:balance
    pub async fn get_balance(&mut self, user_id: u64, asset_id: u16) -> TestResult<RedisBalance> {
        let key = format!("user:{}:asset:{}:balance", user_id, asset_id);

        let result: HashMap<String, String> = self.conn.hgetall(&key).await?;

        if result.is_empty() {
            return Err(TestError::Verification {
                message: format!(
                    "Balance not found in Redis for user {} asset {}",
                    user_id, asset_id
                ),
            });
        }

        Ok(RedisBalance {
            user_id,
            asset_id,
            available: result
                .get("available")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            locked: result
                .get("locked")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            total: result
                .get("total")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            timestamp: result
                .get("timestamp")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
        })
    }

    /// Get order details from Redis
    /// Key format: order:{order_id}
    pub async fn get_order(&mut self, order_id: u64) -> TestResult<Option<RedisOrder>> {
        let key = format!("order:{}", order_id);

        let result: HashMap<String, String> = self.conn.hgetall(&key).await?;

        if result.is_empty() {
            return Ok(None);
        }

        Ok(Some(RedisOrder {
            order_id: result
                .get("order_id")
                .and_then(|v| v.parse().ok())
                .unwrap_or(order_id),
            user_id: result
                .get("user_id")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            price: result
                .get("price")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            size: result.get("size").and_then(|v| v.parse().ok()).unwrap_or(0),
            side: result.get("side").cloned().unwrap_or_default(),
            timestamp: result
                .get("timestamp")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            market_id: result
                .get("market_id")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            status: result.get("status").cloned().unwrap_or_default(),
        }))
    }

    /// Get recent trades from Redis ZSET
    /// Key format: market:{market_id}:trades
    pub async fn get_trades(
        &mut self,
        market_id: u32,
        count: usize,
    ) -> TestResult<Vec<RedisTrade>> {
        let key = format!("market:{}:trades", market_id);

        // Get from sorted set (most recent first)
        let trades: Vec<String> = self.conn.zrevrange(&key, 0, (count as isize) - 1).await?;

        info!(
            "Fetched {} trades from Redis for market {}",
            trades.len(),
            market_id
        );

        let mut result = Vec::new();
        for trade_json in trades {
            match serde_json::from_str::<serde_json::Value>(&trade_json) {
                Ok(trade_val) => {
                    let trade = RedisTrade {
                        trade_id: format!(
                            "{}:{}:{}",
                            trade_val["taker_order_id"].as_u64().unwrap_or(0),
                            trade_val["maker_order_id"].as_u64().unwrap_or(0),
                            trade_val["timestamp"].as_u64().unwrap_or(0)
                        ),
                        maker_user_id: trade_val["maker_user_id"].as_u64().unwrap_or(0),
                        taker_user_id: trade_val["taker_user_id"].as_u64().unwrap_or(0),
                        market_id: trade_val["market_id"].as_u64().unwrap_or(0) as u32,
                        price: trade_val["price"].as_u64().unwrap_or(0),
                        size: trade_val["size"].as_u64().unwrap_or(0),
                        maker_order_id: trade_val["maker_order_id"].as_u64().unwrap_or(0),
                        taker_order_id: trade_val["taker_order_id"].as_u64().unwrap_or(0),
                        timestamp: trade_val["timestamp"].as_u64().unwrap_or(0),
                    };
                    result.push(trade);
                }
                Err(e) => {
                    warn!("Failed to parse trade JSON: {}", e);
                }
            }
        }

        Ok(result)
    }

    /// Get orderbook snapshot from Redis
    /// Key format: orderbook:market:{market_id}
    /// Returns empty orderbook if key doesn't exist
    pub async fn get_orderbook(&mut self, market_id: u32) -> TestResult<RedisOrderbook> {
        let key = format!("orderbook:market:{}", market_id);

        let orderbook_json: Option<String> = self.conn.get(&key).await?;

        // If key doesn't exist, return empty orderbook
        let orderbook_json = match orderbook_json {
            Some(json) => json,
            None => {
                debug!(
                    "Orderbook key not found for market {}, returning empty orderbook",
                    market_id
                );
                return Ok(RedisOrderbook {
                    market_id,
                    bids: vec![],
                    asks: vec![],
                    timestamp: 0,
                });
            }
        };

        let ob: serde_json::Value =
            serde_json::from_str(&orderbook_json).map_err(|e| TestError::Parse {
                message: format!("Failed to parse orderbook JSON: {}", e),
            })?;

        let bids: Vec<OrderbookLevel> = ob["bids"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|level| {
                Some(OrderbookLevel {
                    price: level["price"].as_u64()?,
                    size: level["size"].as_u64()?,
                })
            })
            .collect();

        let asks: Vec<OrderbookLevel> = ob["asks"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|level| {
                Some(OrderbookLevel {
                    price: level["price"].as_u64()?,
                    size: level["size"].as_u64()?,
                })
            })
            .collect();

        Ok(RedisOrderbook {
            market_id: ob["market_id"].as_u64().unwrap_or(market_id as u64) as u32,
            bids,
            asks,
            timestamp: ob["timestamp"].as_u64().unwrap_or(0),
        })
    }

    /// Check if order exists in active orders set
    /// Set key: market:{market_id}:active_orders
    pub async fn verify_order_in_active(
        &mut self,
        market_id: u32,
        order_id: u64,
    ) -> TestResult<bool> {
        let key = format!("market:{}:active_orders", market_id);
        let order_id_str = order_id.to_string();

        let exists: bool = self.conn.sismember(&key, &order_id_str).await?;
        Ok(exists)
    }

    /// Check if order exists in cancelled orders set
    /// Set key: market:{market_id}:cancelled_orders
    pub async fn verify_order_in_cancelled(
        &mut self,
        market_id: u32,
        order_id: u64,
    ) -> TestResult<bool> {
        let key = format!("market:{}:cancelled_orders", market_id);
        let order_id_str = order_id.to_string();

        let exists: bool = self.conn.sismember(&key, &order_id_str).await?;
        Ok(exists)
    }

    /// Wait for balance event to appear in Redis (with timeout)
    pub async fn wait_for_balance_update(
        &mut self,
        user_id: u64,
        asset_id: u16,
        wait_timeout: Duration,
    ) -> TestResult<RedisBalance> {
        let start = std::time::Instant::now();

        loop {
            match self.get_balance(user_id, asset_id).await {
                Ok(balance) => return Ok(balance),
                Err(_) => {
                    if start.elapsed() > wait_timeout {
                        return Err(TestError::Timeout {
                            timeout: wait_timeout,
                        });
                    }
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    /// Wait for order to appear in Redis (with timeout)
    pub async fn wait_for_order(
        &mut self,
        order_id: u64,
        wait_timeout: Duration,
    ) -> TestResult<RedisOrder> {
        let start = std::time::Instant::now();

        loop {
            match self.get_order(order_id).await {
                Ok(Some(order)) => return Ok(order),
                _ => {
                    if start.elapsed() > wait_timeout {
                        return Err(TestError::Timeout {
                            timeout: wait_timeout,
                        });
                    }
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    /// Wait for trade matching criteria to appear in Redis (with timeout)
    /// Searches through recent trades to find one matching the provided criteria
    pub async fn wait_for_trade(
        &mut self,
        market_id: u32,
        criteria: &TradeCriteria,
        wait_timeout: Duration,
    ) -> TestResult<RedisTrade> {
        let start = std::time::Instant::now();

        loop {
            // Fetch more trades to increase chances of finding a match
            match self.get_trades(market_id, 100).await {
                Ok(trades) => {
                    // Search for a trade matching the criteria
                    if let Some(trade) = trades.iter().find(|t| criteria.matches(t)) {
                        return Ok(trade.clone());
                    }
                }
                Err(e) => {
                    debug!("Error fetching trades: {}", e);
                }
            }

            if start.elapsed() > wait_timeout {
                return Err(TestError::Timeout {
                    timeout: wait_timeout,
                });
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    /// Wait for orderbook update (with timeout)
    /// Waits for orderbook to have non-zero timestamp (indicating it's been published)
    pub async fn wait_for_orderbook_update(
        &mut self,
        market_id: u32,
        wait_timeout: Duration,
    ) -> TestResult<RedisOrderbook> {
        timeout(wait_timeout, async {
            loop {
                match self.get_orderbook(market_id).await {
                    Ok(ob) if ob.timestamp > 0 => {
                        // Orderbook has been published (non-zero timestamp)
                        return Ok(ob);
                    }
                    Ok(_) => {
                        // Empty orderbook (not yet published), keep waiting
                        debug!(
                            "Orderbook for market {} not yet published, waiting...",
                            market_id
                        );
                        sleep(Duration::from_millis(50)).await;
                    }
                    Err(_) => sleep(Duration::from_millis(50)).await,
                }
            }
        })
        .await
        .map_err(|_| TestError::Timeout {
            timeout: wait_timeout,
        })?
    }

    /// Cleanup test data from Redis
    /// Removes all keys matching test patterns
    pub async fn cleanup_test_data(&mut self, prefix: &str) -> TestResult<()> {
        // Scan and delete keys matching prefix
        let pattern = format!("{}*", prefix);

        let mut cursor = 0u64;
        loop {
            let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut self.conn)
                .await?;

            if !keys.is_empty() {
                redis::cmd("DEL")
                    .arg(&keys)
                    .query_async(&mut self.conn)
                    .await
                    .map(|_: ()| ())?;

                debug!("Deleted {} keys matching pattern {}", keys.len(), pattern);
            }

            cursor = new_cursor;
            if cursor == 0 {
                break;
            }
        }

        Ok(())
    }
}

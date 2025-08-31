use common::MatcherTradeEvent;
use common::ProcessedOrderCommand;
use common::Status;
use common::Order;
use common::L2MarketData;
use crate::risk_engine::RiskEngine;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tracing::{error, info};

pub trait EventsHandler: Send + Sync {
    fn handle_processed_command(&self, processed_cmd: &ProcessedOrderCommand, risk_engine: Option<&RiskEngine>, orderbook_snapshot: Option<L2MarketData<50>>);
}

// Mock Kafka Events Handler - can be replaced with real Kafka implementation
pub struct KafkaEventsHandler {
    published_events: Arc<Mutex<Vec<String>>>,
}

impl KafkaEventsHandler {
    pub fn new() -> Self {
        Self {
            published_events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn publish_balance_event(&self, user_id: u64, market_id: u32, risk_engine: &RiskEngine) -> Result<(), String> {
        // Get user balance from risk engine
        if let Some(user_profile) = risk_engine.user_balances.get(&user_id) {
            if let Ok(balance) = user_profile.get_balance(user_id, market_id) {
                let balance_event = BalanceEvent {
                    user_id,
                    market_id,
                    available: balance.available(),
                    locked: balance.locked(),
                    total: balance.total(),
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                };

                let payload = serde_json::to_string(&balance_event)
                    .map_err(|e| format!("Failed to serialize balance event: {}", e))?;

                // Mock Kafka publish - in real implementation, this would send to Kafka
                let mut events = self.published_events.lock().unwrap();
                events.push(format!("BALANCE: {}", payload));
                
                info!("[KafkaEventsHandler] Published balance event for user {}", user_id);
            }
        }

        Ok(())
    }

    fn publish_order_event(&self, processed_cmd: &ProcessedOrderCommand) -> Result<(), String> {
        // Create Order struct from ProcessedOrderCommand with actual price and size
        let order = Order {
            order_id: processed_cmd.order_id(),
            user_id: processed_cmd.taker_id(),
            price: processed_cmd.price(), // Use actual price from ProcessedOrderCommand
            size: processed_cmd.size(),   // Use actual size from ProcessedOrderCommand
            side: processed_cmd.side(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        };

        let order_event = OrderEvent {
            order: order,
            market_id: processed_cmd.market_id(),
        };

        let payload = serde_json::to_string(&order_event)
            .map_err(|e| format!("Failed to serialize order event: {}", e))?;

        // Mock Kafka publish
        let mut events = self.published_events.lock().unwrap();
        events.push(format!("ORDER: {}", payload));
        
        info!("[KafkaEventsHandler] Published order event for order {}", processed_cmd.order_id());

        Ok(())
    }

    fn publish_trade_event(&self, event: &MatcherTradeEvent, market_id: u32, taker_id: u64, taker_order_id: u64) -> Result<(), String> {
        let trade_event = TradeEvent {
            maker_user_id: event.maker_user_id,
            taker_user_id: taker_id,
            market_id,
            price: event.price,
            size: event.size,
            maker_order_id: event.matched_order_id, // This is the passive/maker order ID
            taker_order_id, // This is the active/taker order ID
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        };

        let payload = serde_json::to_string(&trade_event)
            .map_err(|e| format!("Failed to serialize trade event: {}", e))?;

        // Mock Kafka publish
        let mut events = self.published_events.lock().unwrap();
        events.push(format!("TRADE: {}", payload));
        
        info!("[KafkaEventsHandler] Published trade event for maker order {} and taker order {}", 
              event.matched_order_id, taker_order_id);

        Ok(())
    }

    fn publish_cancel_order_event(&self, processed_cmd: &ProcessedOrderCommand) -> Result<(), String> {
        let cancel_event = CancelOrderEvent {
            order_id: processed_cmd.order_id(),
            market_id: processed_cmd.market_id(),
            user_id: processed_cmd.taker_id(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        };

        let payload = serde_json::to_string(&cancel_event)
            .map_err(|e| format!("Failed to serialize cancel order event: {}", e))?;

        // Mock Kafka publish
        let mut events = self.published_events.lock().unwrap();
        events.push(format!("CANCEL_ORDER: {}", payload));
        
        info!("[KafkaEventsHandler] Published cancel order event for order {}", processed_cmd.order_id());

        Ok(())
    }

    fn publish_orderbook_event(&self, market_id: u32, orderbook_snapshot: Option<L2MarketData<50>>) -> Result<(), String> {
        if let Some(snapshot) = orderbook_snapshot {
            // Convert L2MarketData to serializable format
            let mut bids = Vec::new();
            let mut asks = Vec::new();
            
            // Convert bid data (only non-zero prices)
            for i in 0..snapshot.depth() {
                if snapshot.bid_prices[i] > 0 {
                    bids.push(OrderbookLevel {
                        price: snapshot.bid_prices[i],
                        size: snapshot.bid_volumes[i],
                    });
                }
            }
            
            // Convert ask data (only non-zero prices)
            for i in 0..snapshot.depth() {
                if snapshot.ask_prices[i] > 0 {
                    asks.push(OrderbookLevel {
                        price: snapshot.ask_prices[i],
                        size: snapshot.ask_volumes[i],
                    });
                }
            }
            
            let orderbook_event = OrderbookEvent {
                market_id,
                bids,
                asks,
                timestamp: snapshot.timestamp,
            };

            let bid_count = orderbook_event.bids.len();
            let ask_count = orderbook_event.asks.len();

            let payload = serde_json::to_string(&orderbook_event)
                .map_err(|e| format!("Failed to serialize orderbook event: {}", e))?;

            // Mock Kafka publish
            let mut events = self.published_events.lock().unwrap();
            events.push(format!("ORDERBOOK: {}", payload));
            
            info!("[KafkaEventsHandler] Published orderbook event for market {} with {} bid levels and {} ask levels", 
                  market_id, bid_count, ask_count);

            Ok(())
        } else {
            Ok(())
        }
    }

    // Method to get published events for testing
    pub fn get_published_events(&self) -> Vec<String> {
        self.published_events.lock().unwrap().clone()
    }
}

impl EventsHandler for KafkaEventsHandler {
    fn handle_processed_command(&self, processed_cmd: &ProcessedOrderCommand, risk_engine: Option<&RiskEngine>, orderbook_snapshot: Option<L2MarketData<50>>) {
        info!(
            "[KafkaEventsHandler] Processing command: Order {}, Status {:?}",
            processed_cmd.order_id(),
            processed_cmd.status()
        );

        let market_id = processed_cmd.market_id();
        let taker_id = processed_cmd.taker_id();
        let taker_order_id = processed_cmd.order_id();

        match processed_cmd.status() {
            Status::Rejected => {
                // Nothing is published to Kafka for rejected orders
                info!("[KafkaEventsHandler] Order {} rejected - no events published", processed_cmd.order_id());
            }
            Status::Placed => {
                // Publish order event and orderbook event
                if let Err(e) = self.publish_order_event(processed_cmd) {
                    error!("[KafkaEventsHandler] Failed to publish order event: {}", e);
                }
                if let Err(e) = self.publish_orderbook_event(market_id, orderbook_snapshot) {
                    error!("[KafkaEventsHandler] Failed to publish orderbook event: {}", e);
                }
            }
            Status::Cancelled => {
                // Publish balance event and cancel order event
                if let Some(risk_engine) = risk_engine {
                    if let Err(e) = self.publish_balance_event(taker_id, market_id, risk_engine) {
                        error!("[KafkaEventsHandler] Failed to publish balance event: {}", e);
                    }
                }
                if let Err(e) = self.publish_cancel_order_event(processed_cmd) {
                    error!("[KafkaEventsHandler] Failed to publish cancel order event: {}", e);
                }
                if let Err(e) = self.publish_orderbook_event(market_id, orderbook_snapshot) {
                    error!("[KafkaEventsHandler] Failed to publish orderbook event: {}", e);
                }
            }
            Status::PartiallyFilled | Status::Filled => {
                // Process all trade events in the linked list
                if let Some(event) = processed_cmd.events() {
                    // Process the main event
                    if let Err(e) = self.publish_trade_event(event, market_id, taker_id, taker_order_id) {
                        error!("[KafkaEventsHandler] Failed to publish trade event: {}", e);
                    }

                    // Process all chained events
                    let mut current_event = event.next_event.as_ref();
                    while let Some(next_event) = current_event {
                        if let Err(e) = self.publish_trade_event(next_event, market_id, taker_id, taker_order_id) {
                            error!("[KafkaEventsHandler] Failed to publish chained trade event: {}", e);
                        }
                        current_event = next_event.next_event.as_ref();
                    }

                    // Publish balance events for all makers and taker involved
                    if let Some(risk_engine) = risk_engine {
                        // Publish balance for taker
                        if let Err(e) = self.publish_balance_event(taker_id, market_id, risk_engine) {
                            error!("[KafkaEventsHandler] Failed to publish taker balance event: {}", e);
                        }

                        // Publish balance for main event maker
                        if let Err(e) = self.publish_balance_event(event.maker_user_id, market_id, risk_engine) {
                            error!("[KafkaEventsHandler] Failed to publish main maker balance event: {}", e);
                        }

                        // Publish balance for all chained event makers
                        let mut current_event = event.next_event.as_ref();
                        while let Some(next_event) = current_event {
                            if let Err(e) = self.publish_balance_event(next_event.maker_user_id, market_id, risk_engine) {
                                error!("[KafkaEventsHandler] Failed to publish chained maker balance event: {}", e);
                            }
                            current_event = next_event.next_event.as_ref();
                        }
                    }
                }
                
                // Publish orderbook event
                if let Err(e) = self.publish_orderbook_event(market_id, orderbook_snapshot) {
                    error!("[KafkaEventsHandler] Failed to publish orderbook event: {}", e);
                }
            }
        }
    }
}

// Event structures for Kafka messages
#[derive(Serialize, Deserialize, Debug)]
struct BalanceEvent {
    user_id: u64,
    market_id: u32,
    available: u64,
    locked: u64,
    total: u64,
    timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct OrderEvent {
    order: Order,
    market_id: u32,
}

#[derive(Serialize, Deserialize, Debug)]
struct TradeEvent {
    maker_user_id: u64,
    taker_user_id: u64,
    market_id: u32,
    price: u64,
    size: u64,
    maker_order_id: u64, // Passive order ID (from matched_order_id)
    taker_order_id: u64, // Active order ID
    timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct CancelOrderEvent {
    order_id: u64,
    market_id: u32,
    user_id: u64,
    timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct OrderbookEvent {
    market_id: u32,
    bids: Vec<OrderbookLevel>,
    asks: Vec<OrderbookLevel>,
    timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct OrderbookLevel {
    price: u64,
    size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::Side;
    use vex_orderbook::OrderBook;
    use vex_orderbook::tree::{BTreeAskSide, BTreeBidSide};

    #[test]
    fn test_kafka_events_handler_placed_order() {
        let handler = KafkaEventsHandler::new();
        
        // Create a processed command with Placed status
        let processed_cmd = ProcessedOrderCommand::new(
            Status::Placed,
            12345,
            1001,
            1,
            1000, // price
            100,  // size
            Side::Bid,
        );
        
        // Handle the processed command
        handler.handle_processed_command(&processed_cmd, None, None);
        
        // Check that events were published
        let events = handler.get_published_events();
        assert_eq!(events.len(), 1); // Only Order event (no orderbook event since no snapshot provided)
        
        // Verify order event was published
        assert!(events.iter().any(|e| e.starts_with("ORDER:")));
    }

    #[test]
    fn test_kafka_events_handler_cancelled_order() {
        let handler = KafkaEventsHandler::new();
        
        // Create a processed command with Cancelled status
        let processed_cmd = ProcessedOrderCommand::new(
            Status::Cancelled,
            12346,
            1002,
            1,
            950,  // price
            50,   // size
            Side::Ask,
        );
        
        // Handle the processed command
        handler.handle_processed_command(&processed_cmd, None, None);
        
        // Check that events were published
        let events = handler.get_published_events();
        assert_eq!(events.len(), 1); // Only Cancel order event (no orderbook event since no snapshot provided)
        
        // Verify cancel order event was published
        assert!(events.iter().any(|e| e.starts_with("CANCEL_ORDER:")));
    }

    #[test]
    fn test_kafka_events_handler_rejected_order() {
        let handler = KafkaEventsHandler::new();
        
        // Create a processed command with Rejected status
        let processed_cmd = ProcessedOrderCommand::new(
            Status::Rejected,
            12347,
            1003,
            1,
            1100, // price
            75,   // size
            Side::Bid,
        );
        
        // Handle the processed command
        handler.handle_processed_command(&processed_cmd, None, None);
        
        // Check that no events were published for rejected orders
        let events = handler.get_published_events();
        assert_eq!(events.len(), 0);
    }

    #[test]
    fn test_kafka_events_handler_filled_order() {
        let handler = KafkaEventsHandler::new();
        
        // Create a processed command with Filled status and trade event
        let processed_cmd = ProcessedOrderCommand::new(
            Status::Filled,
            12348,
            1004,
            1,
            1050, // price
            200,  // size
            Side::Bid,
        );
        
        // Handle the processed command
        handler.handle_processed_command(&processed_cmd, None, None);
        
        // Check that events were published
        let events = handler.get_published_events();
        assert_eq!(events.len(), 0); // No events since no trade events and no orderbook snapshot
        
        // Verify no events were published
        assert!(events.is_empty());
    }

    #[test]
    fn test_kafka_events_handler_with_risk_engine() {
        use hashbrown::HashMap;
        use common::{BalanceStore, UserBalance};
        
        // Create a risk engine with some user balances
        let mut risk_engine = RiskEngine::new(HashMap::new(), 0, 1);
        
        // Add a user with some balance
        let mut user_profile = BalanceStore::new();
        let balance = UserBalance::new();
        user_profile.set_balance(1001, 1, balance);
        risk_engine.user_balances.insert(1001, user_profile);
        
        let handler = KafkaEventsHandler::new();
        
        // Create a processed command with Cancelled status
        let processed_cmd = ProcessedOrderCommand::new(
            Status::Cancelled,
            12349,
            1001,
            1,
            900,  // price
            25,   // size
            Side::Ask,
        );
        
        // Handle the processed command with risk engine
        handler.handle_processed_command(&processed_cmd, Some(&risk_engine), None);
        
        // Check that events were published
        let events = handler.get_published_events();
        assert_eq!(events.len(), 2); // Balance event + Cancel order event (no orderbook event since no snapshot provided)
        
        // Verify balance event was published
        assert!(events.iter().any(|e| e.starts_with("BALANCE:")));
        
        // Verify cancel order event was published
        assert!(events.iter().any(|e| e.starts_with("CANCEL_ORDER:")));
    }

    #[test]
    fn test_orderbook_event_with_real_orderbook() {
        let handler = KafkaEventsHandler::new();
        
        // Create a real orderbook with some orders
        let mut orderbook = OrderBook::new(BTreeBidSide::new(), BTreeAskSide::new());
        
        // Add some orders to the orderbook
        let bid_cmd = common::OrderCommand {
            command: common::OrderCommandType::PlaceOrder,
            order_id: 1,
            timestamp: 100,
            user_id: 1001,
            market_id: 1,
            price: 1000,
            size: 100,
            side: Side::Bid,
            time_in_force: common::TimeInForce::Gtc,
        };
        orderbook.place_order(&bid_cmd);
        
        let ask_cmd = common::OrderCommand {
            command: common::OrderCommandType::PlaceOrder,
            order_id: 2,
            timestamp: 101,
            user_id: 1002,
            market_id: 1,
            price: 1100,
            size: 50,
            side: Side::Ask,
            time_in_force: common::TimeInForce::Gtc,
        };
        orderbook.place_order(&ask_cmd);
        
        // Create a processed command
        let processed_cmd = ProcessedOrderCommand::new(
            Status::Placed,
            12350,
            1003,
            1,
            1050, // price
            75,   // size
            Side::Bid,
        );
        
        // Handle the processed command with orderbook
        let snapshot = orderbook.create_snapshot_with_depth(50);
        handler.handle_processed_command(&processed_cmd, None, Some(snapshot));
        
        // Check that events were published
        let events = handler.get_published_events();
        assert_eq!(events.len(), 2); // Order event + Orderbook event
        
        // Verify orderbook event was published and contains real data
        let orderbook_event = events.iter().find(|e| e.starts_with("ORDERBOOK:")).unwrap();
        assert!(orderbook_event.contains("1000")); // Should contain bid price
        assert!(orderbook_event.contains("1100")); // Should contain ask price
        assert!(orderbook_event.contains("100"));  // Should contain bid size
        assert!(orderbook_event.contains("50"));   // Should contain ask size
    }
}
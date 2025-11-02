use common::MatcherTradeEvent;
use std::sync::{Arc, Mutex};
use tracing::info;

pub trait EventsHandler: Send + Sync {
    fn handle_event(&self, event: MatcherTradeEvent);
}

#[derive(Clone, Default)]
pub struct SimpleEventsHandler {
    pub events: Arc<Mutex<Vec<MatcherTradeEvent>>>,
}

impl SimpleEventsHandler {
    pub fn new() -> Self {
        Self::default()
    }
}

impl EventsHandler for SimpleEventsHandler {
    fn handle_event(&self, event: MatcherTradeEvent) {
        let mut events = self.events.lock().unwrap();
        info!(
            "[SimpleEventsHandler] Received final event: Price {}, Size {}, Matched Order ID {}",
            event.price, event.size, event.matched_order_id
        );
        events.push(event);
    }
}

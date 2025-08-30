use common::cmd::MatcherTradeEvent;
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
        let mut events = match self.events.lock() {
            Ok(events) => events,
            Err(poisoned) => {
                tracing::warn!("Events mutex was poisoned, recovering data");
                poisoned.into_inner()
            }
        };
        info!(
            "[SimpleEventsHandler] Received final event: {:?}",
            event.event_type
        );
        events.push(event);
    }
}

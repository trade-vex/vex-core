use common::cmd::MatcherTradeEvent;
use std::sync::{Arc, Mutex};
use tracing::info;
// #[async_trait]
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

// #[async_trait]
impl EventsHandler for SimpleEventsHandler {
    fn handle_event(&self, event: MatcherTradeEvent) {
        let mut events = self.events.lock().unwrap();
        info!(
            "[SimpleEventsHandler] Received final event: {:?}",
            event.event_type
        );
        events.push(event);
    }
}

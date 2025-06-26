use common::cmd::OrderCommand;
use hashbrown::HashMap;
use orderbook::direct_impl::OrderBookDirectImpl;
use orderbook::naive_impl::OrderBookNaiveImpl;
use orderbook::OrderBook;
use orderbook::OrderBookImplType;

/// Owns all order books and routes commands to the correct one.
/// This is the Rust equivalent of `MatchingEngineRouter.java`.
pub struct MatchingEngineRouter {
    order_books: HashMap<i32, Box<dyn OrderBook<'static> + Send>>,
}

impl MatchingEngineRouter {
    pub fn new() -> Self {
        Self {
            order_books: HashMap::new(),
        }
    }

    /// Adds a new symbol to the matching engine, creating a new order book for it.
    pub fn add_symbol(&mut self, symbol_id: i32, book_type: OrderBookImplType) {
        let spec = common::model::symbol_specification::TestConstants::symbol_spec_eth_xbt();
        let book: Box<dyn OrderBook + Send> = match book_type {
            OrderBookImplType::Naive => Box::new(OrderBookNaiveImpl::new(spec)),
            OrderBookImplType::Direct => Box::new(OrderBookDirectImpl::new(spec)),
        };
        self.order_books.insert(symbol_id, book);
    }

    /// Routes a command to the appropriate order book for processing.
    /// This is the core matching stage(Excali-6b) of the pipeline.
    pub fn route_command(&mut self, cmd: &mut OrderCommand) {
        if let Some(order_book) = self.order_books.get_mut(&cmd.symbol) {
            println!(
                "[Router] Routing command for symbol {} to its order book.",
                cmd.symbol
            );
            // Events are created inside these methods(excali-7)
            let result = match cmd.command {
                common::cmd::OrderCommandType::PlaceOrder => order_book.new_order(cmd),
                common::cmd::OrderCommandType::CancelOrder => order_book.cancel_order(cmd),
                common::cmd::OrderCommandType::MoveOrder => order_book.move_order(cmd),
                common::cmd::OrderCommandType::ReduceOrder => order_book.reduce_order(cmd),
            };
            println!("[Router] Order book processed command: {:?}", cmd);

            if let Err(e) = result {
                tracing::warn!("[Router] Order book processing failed: {:?}", e);
            }
        } else {
            tracing::warn!("[Router] No order book found for symbol {}", cmd.symbol);
        }
    }
}

impl Default for MatchingEngineRouter {
    fn default() -> Self {
        Self::new()
    }
}

//! The engine's state in a snapshot: everything a stage holds between commands. The rest is
//! derived (price levels' totals, the order index, the price cache) or comes from the config and
//! the journal (markets, the id and timestamp counters).

use crate::engine::{EngineError, EngineResult, Routers};
use borsh::{BorshDeserialize, BorshSerialize};
use common::{Order, PriceCache};
use processors::{
    matching_engine::MatchingEngineRouter,
    risk_engine::{RiskEngine, RiskShardState},
};

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub struct EngineState {
    /// One per risk shard, in shard order.
    pub risk: Vec<RiskShardState>,
    /// Every market's resting orders, by market id: bids high to low, then asks low to high,
    /// FIFO within a level.
    pub books: Vec<(u32, Vec<Order>)>,
}

impl EngineState {
    /// Serializes the shards' state. Call only while the pipeline is drained.
    pub fn capture(risk: &[RiskEngine], routers: &Routers) -> Vec<u8> {
        let mut books: Vec<(u32, Vec<Order>)> = routers
            .iter()
            .flat_map(|router| {
                let router = router.lock();
                router
                    .order_books
                    .iter()
                    .map(|(id, book)| (*id, book.resting_orders().copied().collect()))
                    .collect::<Vec<_>>()
            })
            .collect();
        // A handful of markets; the balances are not sorted, which would lengthen the pause.
        books.sort_unstable_by_key(|(id, _)| *id);
        let state = Self {
            risk: risk.iter().map(RiskEngine::export).collect(),
            books,
        };
        borsh::to_vec(&state).expect("serializing to memory cannot fail")
    }

    /// Restores `bytes` into shards and routers that have processed nothing yet.
    pub fn restore(
        bytes: &[u8],
        risk: &[RiskEngine],
        routers: &mut [MatchingEngineRouter],
        price_cache: &PriceCache,
    ) -> EngineResult<()> {
        let state = Self::try_from_slice(bytes).map_err(|error| {
            EngineError::ServerInitialization(format!("snapshot state: {error}"))
        })?;
        if state.risk.len() != risk.len() {
            return Err(EngineError::Configuration(format!(
                "the snapshot has {} risk shards, this build has {}",
                state.risk.len(),
                risk.len()
            )));
        }
        for (engine, shard) in risk.iter().zip(state.risk) {
            engine.import(shard);
        }
        for (market_id, orders) in state.books {
            let book = routers
                .iter_mut()
                .find_map(|router| router.order_books.get_mut(&market_id))
                .ok_or_else(|| {
                    EngineError::Configuration(format!(
                        "the snapshot holds market {market_id}, which the config does not; a \
                         market removed with resting orders needs them cancelled first"
                    ))
                })?;
            book.restore(orders, price_cache);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::OrderProducer;
    use crate::test::{PIPELINE_DEADLINE, build};
    use common::{
        BalanceKey, CoreMarketSpecification, MarketType, OrderCommand, Side, Status, TimeInForce,
        UserBalance,
    };
    use disruptor::Producer;
    use hashbrown::HashMap;
    use processors::journaling::ReplayControl;
    use std::sync::mpsc::Receiver;

    const MARKET: u32 = (2 << 16) | 1;
    const BASE: u16 = 1;
    const QUOTE: u16 = 2;

    fn specs() -> HashMap<u32, CoreMarketSpecification> {
        let spec = CoreMarketSpecification::builder()
            .market_id(MARKET)
            .market_type(MarketType::Spot)
            .maker_fee(10)
            .taker_fee(20)
            .slippage(5)
            .build()
            .unwrap();
        HashMap::from([(MARKET, spec)])
    }

    /// Runs `script` one command at a time, so that every balance a response reports is final,
    /// each stamped with the order id and timestamp `first_id`, `first_id + 1`, ... (a cancel
    /// keeps the order id it names). Returns the responses.
    fn run(
        (producer, receiver): &mut (OrderProducer, Receiver<OrderCommand>),
        first_id: u64,
        script: &[OrderCommand],
    ) -> Vec<OrderCommand> {
        let mut responses = Vec::new();
        for (k, cmd) in script.iter().enumerate() {
            let mut cmd = cmd.clone();
            cmd.timestamp = first_id + k as u64;
            if cmd.order_id == 0 {
                cmd.order_id = cmd.timestamp;
            }
            producer.publish(|slot| *slot = cmd);
            responses.push(receiver.recv_timeout(PIPELINE_DEADLINE).unwrap());
        }
        responses
    }

    fn place(user: u64, price: u64, size: u64, side: Side) -> OrderCommand {
        OrderCommand::place_order(TimeInForce::Gtc, user, price, size, side, MARKET, 0)
    }

    type Shard = (HashMap<BalanceKey, UserBalance>, HashMap<u64, u64>);

    /// The state, each shard's maps compared as maps.
    fn parsed(bytes: &[u8]) -> (Vec<Shard>, Vec<(u32, Vec<Order>)>) {
        let state = EngineState::try_from_slice(bytes).unwrap();
        let risk = state
            .risk
            .into_iter()
            .map(|shard| {
                (
                    shard.balances.into_iter().collect(),
                    shard.bid_collateral.into_iter().collect(),
                )
            })
            .collect();
        (risk, state.books)
    }

    #[test]
    fn snapshot_plus_replay_equals_full_replay() {
        let (bidder, asker) = (5_u64, 6_u64); // shards 1 and 2
        // Two deposits, three bids resting at three prices, an ask that partly fills the best
        // bid, an ask that rests, and a cancel of the lowest bid.
        let s1 = [
            OrderCommand::deposit_funds(bidder, 1_000_000_000, QUOTE),
            OrderCommand::deposit_funds(asker, 1_000_000, BASE),
            place(bidder, 5_000, 300, Side::Bid),
            place(bidder, 4_900, 300, Side::Bid),
            place(bidder, 4_800, 300, Side::Bid),
            place(asker, 5_000, 100, Side::Ask),
            place(asker, 5_100, 100, Side::Ask),
            OrderCommand::cancel_order(5, bidder, Side::Bid, MARKET),
        ];
        // A market buy, which R1 caps at the best ask in the price cache, before any command
        // updates it; an ask sweeping two levels; a withdrawal; a bid that rests.
        let market_buy =
            OrderCommand::place_order(TimeInForce::Ioc, bidder, u64::MAX, 50, Side::Bid, MARKET, 0);
        let s2 = [
            market_buy,
            place(asker, 4_900, 400, Side::Ask),
            OrderCommand::withdraw_funds(asker, 1_000, BASE),
            place(bidder, 4_700, 50, Side::Bid),
        ];

        let (a, mut a_pipeline) = {
            let (engine, producer, receiver) = build(specs(), ReplayControl::enabled(), None);
            (engine, (producer, receiver))
        };
        let statuses = run(&mut a_pipeline, 1, &s1)
            .iter()
            .map(|r| r.status)
            .collect::<Vec<_>>();
        assert_eq!(
            statuses[5..],
            [Status::Filled, Status::Placed, Status::Cancelled]
        );
        let s1_state = EngineState::capture(&a.risk_engines, &a.routers);
        let a_responses = run(&mut a_pipeline, 100, &s2);
        let a_state = EngineState::capture(&a.risk_engines, &a.routers);

        let (b, mut b_pipeline) = {
            let (engine, producer, receiver) =
                build(specs(), ReplayControl::enabled(), Some(&s1_state));
            (engine, (producer, receiver))
        };
        let b_responses = run(&mut b_pipeline, 100, &s2);
        let b_state = EngineState::capture(&b.risk_engines, &b.routers);

        // Every field: status, sizes, balances, fills with their fees, and the book's depth.
        assert_eq!(b_responses, a_responses);
        assert_eq!(
            a_responses.iter().map(|r| r.status).collect::<Vec<_>>(),
            [
                Status::Filled,
                Status::Filled,
                Status::Processed,
                Status::Placed
            ]
        );
        let a_parsed = parsed(&a_state);
        assert_eq!(parsed(&b_state), a_parsed);
        // What rests: the rest of the 4,900 bid, the new bid, the rest of the 5,100 ask; and the
        // quote the bids hold.
        let resting = a_parsed.1[0].1.iter().map(|order| order.order_id);
        assert_eq!(resting.collect::<Vec<_>>(), [4, 103, 7]);
        assert_eq!(a_parsed.0[1].1.len(), 2);
    }

    #[test]
    fn a_snapshot_that_does_not_fit_the_engine_fails_the_start() {
        let (a, producer, receiver) = build(specs(), ReplayControl::enabled(), None);
        run(
            &mut (producer, receiver),
            1,
            &[
                OrderCommand::deposit_funds(5, 1_000_000_000, QUOTE),
                place(5, 5_000, 300, Side::Bid),
            ],
        );
        let state = EngineState::capture(&a.risk_engines, &a.routers);
        let risk: Vec<_> = (0..4)
            .map(|shard| RiskEngine::new(specs(), shard, 4))
            .collect();
        let price_cache = PriceCache::new(specs().keys());

        // A market the config no longer has.
        let mut routers = vec![MatchingEngineRouter::new(0, 1)];
        let error = EngineState::restore(&state, &risk, &mut routers, &price_cache)
            .unwrap_err()
            .to_string();
        assert!(error.contains(&format!("market {MARKET}")), "{error}");
        // A build with another number of risk shards.
        let error = EngineState::restore(&state, &risk[..2], &mut routers, &price_cache)
            .unwrap_err()
            .to_string();
        assert!(error.contains("4 risk shards"), "{error}");
    }
}

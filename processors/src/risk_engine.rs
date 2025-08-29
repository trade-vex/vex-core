use common::model::user_profile::BalanceStore;

pub struct RiskEngine {
    #[allow(dead_code)]
    user_balances: BalanceStore,
}

impl RiskEngine {
    pub fn new() -> Self {
        Self {
            user_balances: BalanceStore::new(),
        }
    }
}

impl Default for RiskEngine {
    fn default() -> Self {
        Self::new()
    }
}

use crate::model::symbol_position_record::SymbolPositionRecord;
use borsh::{BorshDeserialize, BorshSerialize};
use hashbrown::HashMap;

// TODO ...
// positions: IntObjectHashMap<SymbolPositionRecord>
// accounts: IntLongHashMap

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct UserProfile {
    pub uid: i64,
    pub adjustments_counter: i64,
    pub user_status: UserStatus,
    pub positions: HashMap<i32, SymbolPositionRecord>,
    pub accounts: HashMap<i32, i64>,
}

impl UserProfile {
    pub fn new(uid: i64, user_status: UserStatus) -> Self {
        Self {
            uid,
            adjustments_counter: 0,
            user_status,
            positions: HashMap::new(),
            accounts: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum UserStatus {
    Active,
    Suspended,
}

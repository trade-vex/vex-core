use crate::model::symbol_position_record::SymbolPositionRecord;
use borsh::{BorshDeserialize, BorshSerialize, to_vec};
use hashbrown::HashMap;
use std::hash::{Hash, Hasher};
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
/// Implements a state hash for data integrity checks, similar to the Java version.
/// It works by serializing the entire struct into bytes and then hashing those bytes.
impl Hash for UserProfile {

    fn hash<H: Hasher>(&self, state: &mut H) {
        let encoded = to_vec(self).unwrap();
        state.write(&encoded);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum UserStatus {
    Active,
    Suspended,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum OrderCommandType {
    PlaceOrder = 0x0_u8,
    CancelOrder = 0x1_u8,
    DepositFunds = 0x2_u8,
    WithdrawFunds = 0x3_u8,
    AddMarket = 0x4_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for OrderCommandType {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x0_u8 => Self::PlaceOrder,
            0x1_u8 => Self::CancelOrder,
            0x2_u8 => Self::DepositFunds,
            0x3_u8 => Self::WithdrawFunds,
            0x4_u8 => Self::AddMarket,
            _ => Self::NullVal,
        }
    }
}
impl From<OrderCommandType> for u8 {
    #[inline]
    fn from(v: OrderCommandType) -> Self {
        match v {
            OrderCommandType::PlaceOrder => 0x0_u8,
            OrderCommandType::CancelOrder => 0x1_u8,
            OrderCommandType::DepositFunds => 0x2_u8,
            OrderCommandType::WithdrawFunds => 0x3_u8,
            OrderCommandType::AddMarket => 0x4_u8,
            OrderCommandType::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for OrderCommandType {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "PlaceOrder" => Ok(Self::PlaceOrder),
            "CancelOrder" => Ok(Self::CancelOrder),
            "DepositFunds" => Ok(Self::DepositFunds),
            "WithdrawFunds" => Ok(Self::WithdrawFunds),
            "AddMarket" => Ok(Self::AddMarket),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for OrderCommandType {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::PlaceOrder => write!(f, "PlaceOrder"),
            Self::CancelOrder => write!(f, "CancelOrder"),
            Self::DepositFunds => write!(f, "DepositFunds"),
            Self::WithdrawFunds => write!(f, "WithdrawFunds"),
            Self::AddMarket => write!(f, "AddMarket"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}

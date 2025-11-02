#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum OrderCommandType {
    PlaceOrder = 0x0_u8,
    CancelOrder = 0x1_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for OrderCommandType {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x0_u8 => Self::PlaceOrder,
            0x1_u8 => Self::CancelOrder,
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
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}

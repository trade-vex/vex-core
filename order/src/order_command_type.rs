#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum OrderCommandType {
    PlaceLimitOrder = 0x0_u8, 
    PlaceMarketOrder = 0x1_u8, 
    CancelOrder = 0x2_u8, 
    #[default]
    NullVal = 0xff_u8, 
}
impl From<u8> for OrderCommandType {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x0_u8 => Self::PlaceLimitOrder, 
            0x1_u8 => Self::PlaceMarketOrder, 
            0x2_u8 => Self::CancelOrder, 
            _ => Self::NullVal,
        }
    }
}
impl From<OrderCommandType> for u8 {
    #[inline]
    fn from(v: OrderCommandType) -> Self {
        match v {
            OrderCommandType::PlaceLimitOrder => 0x0_u8, 
            OrderCommandType::PlaceMarketOrder => 0x1_u8, 
            OrderCommandType::CancelOrder => 0x2_u8, 
            OrderCommandType::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for OrderCommandType {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "PlaceLimitOrder" => Ok(Self::PlaceLimitOrder), 
            "PlaceMarketOrder" => Ok(Self::PlaceMarketOrder), 
            "CancelOrder" => Ok(Self::CancelOrder), 
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for OrderCommandType {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::PlaceLimitOrder => write!(f, "PlaceLimitOrder"), 
            Self::PlaceMarketOrder => write!(f, "PlaceMarketOrder"), 
            Self::CancelOrder => write!(f, "CancelOrder"), 
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}

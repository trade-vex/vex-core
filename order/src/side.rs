#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Side {
    Ask = 0x0_u8, 
    Bid = 0x1_u8, 
    #[default]
    NullVal = 0xff_u8, 
}
impl From<u8> for Side {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x0_u8 => Self::Ask, 
            0x1_u8 => Self::Bid, 
            _ => Self::NullVal,
        }
    }
}
impl From<Side> for u8 {
    #[inline]
    fn from(v: Side) -> Self {
        match v {
            Side::Ask => 0x0_u8, 
            Side::Bid => 0x1_u8, 
            Side::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for Side {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "Ask" => Ok(Self::Ask), 
            "Bid" => Ok(Self::Bid), 
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for Side {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Ask => write!(f, "Ask"), 
            Self::Bid => write!(f, "Bid"), 
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}

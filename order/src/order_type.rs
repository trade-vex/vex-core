#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum OrderType {
    Gtc = 0x0_u8,
    Ioc = 0x1_u8,
    IocBudget = 0x2_u8,
    Fok = 0x3_u8,
    FokBudget = 0x4_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for OrderType {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x0_u8 => Self::Gtc,
            0x1_u8 => Self::Ioc,
            0x2_u8 => Self::IocBudget,
            0x3_u8 => Self::Fok,
            0x4_u8 => Self::FokBudget,
            _ => Self::NullVal,
        }
    }
}
impl From<OrderType> for u8 {
    #[inline]
    fn from(v: OrderType) -> Self {
        match v {
            OrderType::Gtc => 0x0_u8,
            OrderType::Ioc => 0x1_u8,
            OrderType::IocBudget => 0x2_u8,
            OrderType::Fok => 0x3_u8,
            OrderType::FokBudget => 0x4_u8,
            OrderType::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for OrderType {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "Gtc" => Ok(Self::Gtc),
            "Ioc" => Ok(Self::Ioc),
            "IocBudget" => Ok(Self::IocBudget),
            "Fok" => Ok(Self::Fok),
            "FokBudget" => Ok(Self::FokBudget),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for OrderType {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Gtc => write!(f, "Gtc"),
            Self::Ioc => write!(f, "Ioc"),
            Self::IocBudget => write!(f, "IocBudget"),
            Self::Fok => write!(f, "Fok"),
            Self::FokBudget => write!(f, "FokBudget"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}

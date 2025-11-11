#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Status {
    Rejected = 0x0_u8, 
    Placed = 0x1_u8, 
    Cancelled = 0x2_u8, 
    PartiallyFilled = 0x3_u8, 
    Filled = 0x4_u8, 
    Processing = 0x5_u8, 
    Processed = 0x6_u8, 
    #[default]
    NullVal = 0xff_u8, 
}
impl From<u8> for Status {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x0_u8 => Self::Rejected, 
            0x1_u8 => Self::Placed, 
            0x2_u8 => Self::Cancelled, 
            0x3_u8 => Self::PartiallyFilled, 
            0x4_u8 => Self::Filled, 
            0x5_u8 => Self::Processing, 
            0x6_u8 => Self::Processed, 
            _ => Self::NullVal,
        }
    }
}
impl From<Status> for u8 {
    #[inline]
    fn from(v: Status) -> Self {
        match v {
            Status::Rejected => 0x0_u8, 
            Status::Placed => 0x1_u8, 
            Status::Cancelled => 0x2_u8, 
            Status::PartiallyFilled => 0x3_u8, 
            Status::Filled => 0x4_u8, 
            Status::Processing => 0x5_u8, 
            Status::Processed => 0x6_u8, 
            Status::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for Status {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "Rejected" => Ok(Self::Rejected), 
            "Placed" => Ok(Self::Placed), 
            "Cancelled" => Ok(Self::Cancelled), 
            "PartiallyFilled" => Ok(Self::PartiallyFilled), 
            "Filled" => Ok(Self::Filled), 
            "Processing" => Ok(Self::Processing), 
            "Processed" => Ok(Self::Processed), 
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for Status {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Rejected => write!(f, "Rejected"), 
            Self::Placed => write!(f, "Placed"), 
            Self::Cancelled => write!(f, "Cancelled"), 
            Self::PartiallyFilled => write!(f, "PartiallyFilled"), 
            Self::Filled => write!(f, "Filled"), 
            Self::Processing => write!(f, "Processing"), 
            Self::Processed => write!(f, "Processed"), 
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}

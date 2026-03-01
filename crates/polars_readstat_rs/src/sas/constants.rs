/// Magic number that identifies a SAS7BDAT file
pub const MAGIC_NUMBER: &[u8; 32] = &[
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc2, 0xea, 0x81, 0x60,
    0xb3, 0x14, 0x11, 0xcf, 0xbd, 0x92, 0x08, 0x00, 0x09, 0xc7, 0x31, 0x8c, 0x18, 0x1f, 0x10, 0x11,
];

/// Minimum header size
pub const HEADER_SIZE: usize = 288;

/// SAS epoch: 1960-01-01 (in days before Unix epoch 1970-01-01)
pub const SAS_EPOCH_OFFSET_DAYS: i32 = 3653;

/// Seconds per day for datetime conversion
pub const SECONDS_PER_DAY: i64 = 86400;

/// Compression signature for RLE (Run Length Encoding)
pub const COMPRESSION_SIGNATURE_RLE: &str = "SASYZCRL";

/// Compression signature for RDC (Ross Data Compression)
pub const COMPRESSION_SIGNATURE_RDC: &str = "SASYZCR2";

/// Date and time format names for column type detection
pub const DATETIME_FORMATS: &[&str] = &[
    "DATETIME", "DTWKDATX", "B8601DN", "B8601DT", "B8601DX", "B8601DZ", "B8601LX", "E8601DN",
    "E8601DT", "E8601DX", "E8601DZ", "E8601LX", "DATEAMPM", "DTDATE", "DTMONYY", "DTMONYY",
    "DTWKDATX", "DTYEAR", "TOD", "MDYAMPM",
];

pub const DATE_FORMATS: &[&str] = &[
    "DATE", "DAY", "DDMMYY", "DDMMYYB", "DDMMYYC", "DDMMYYD", "DDMMYYN", "DDMMYYP", "DDMMYYS",
    "JULDAY", "JULIAN", "MMDDYY", "MMDDYYB", "MMDDYYC", "MMDDYYD", "MMDDYYN", "MMDDYYP", "MMDDYYS",
    "MMYY", "MMYYC", "MMYYD", "MMYYN", "MMYYP", "MMYYS", "MONNAME", "MONTH", "MONYY", "QTR",
    "QTRR", "NENGO", "WEEKDATE", "WEEKDATX", "WEEKDAY", "WEEKV", "WORDDATE", "WORDDATX", "YEAR",
    "YYMM", "YYMMC", "YYMMD", "YYMMN", "YYMMP", "YYMMS", "YYMMDD", "YYMMDDB", "YYMMDDC", "YYMMDDD",
    "YYMMDDN", "YYMMDDP", "YYMMDDS", "YYMON", "YYQ", "YYQC", "YYQD", "YYQN", "YYQP", "YYQS",
    "YYQR", "YYQRC", "YYQRD", "YYQRN", "YYQRP", "YYQRS", "YYMMDDP", "YYMMDDS",
];

pub const TIME_FORMATS: &[&str] = &["TIME", "HHMM"];

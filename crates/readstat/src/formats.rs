use lazy_static::lazy_static;
use regex::Regex;

use crate::rs_var::ReadStatVarFormatClass;

pub fn match_var_format(v: &str) -> Option<ReadStatVarFormatClass> {
    lazy_static! {
        static ref RE_DATE: Regex = Regex::new(
            r#"(?xi)
            (^DATE[0-9]{1,2}$) |
            (^DDMMYY[BCDNPS]?[0-9]*$) |
            (^MMDDYY[BCDNPS]?[0-9]*$) |
            (^YYMMDD[BCDNPS]?[0-9]*$)
            "#
        )
        .unwrap();
    };
    lazy_static! {
        static ref RE_DATETIME: Regex = Regex::new(
            r#"(?xi)
            (^DATETIME[0-9]{1,2}$)
            "#
        )
        .unwrap();
    };
    lazy_static! {
        static ref RE_DATETIME_WITH_MILLI: Regex = Regex::new(
            r#"(?xi)
            (^DATETIME[0-9]{1,2}\.\[0-9]{3}$)
            "#
        )
        .unwrap();
    };
    lazy_static! {
        static ref RE_DATETIME_WITH_MICRO: Regex = Regex::new(
            r#"(?xi)
            (^DATETIME[0-9]{1,2}\.[0-9]{6}$)
            "#
        )
        .unwrap();
    };
    lazy_static! {
        static ref RE_DATETIME_WITH_NANO: Regex = Regex::new(
            r#"(?xi)
            (^DATETIME[0-9]{1,2}\.[0-9]{9}$)
            "#
        )
        .unwrap();
    };
    lazy_static! {
        static ref RE_TIME: Regex = Regex::new(
            r#"(?xi)
            (^TIME[0-9]{0,2}$)
            "#
        )
        .unwrap();
    };

    if RE_DATE.is_match(v) {
        Some(ReadStatVarFormatClass::Date)
    } else if RE_DATETIME.is_match(v) {
        Some(ReadStatVarFormatClass::DateTime)
    } else if RE_DATETIME_WITH_MILLI.is_match(v) {
        Some(ReadStatVarFormatClass::DateTimeWithMilliseconds)
    } else if RE_DATETIME_WITH_MICRO.is_match(v) {
        Some(ReadStatVarFormatClass::DateTimeWithMicroseconds)
    } else if RE_DATETIME_WITH_NANO.is_match(v) {
        Some(ReadStatVarFormatClass::DateTimeWithNanoseconds)
    } else if RE_TIME.is_match(v) {
        Some(ReadStatVarFormatClass::Time)
    } else {
        None
    }
}

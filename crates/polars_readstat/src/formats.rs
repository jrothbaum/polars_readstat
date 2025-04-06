use regex::Regex;
use crate::read::ReadstatFileType;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadStatVarFormatClass {
    Date,
    DateTime,
    DateTimeWithMilliseconds,
    DateTimeWithMicroseconds,
    DateTimeWithNanoseconds,
    Time,
    TimeWithMilliseconds,
    TimeWithMicroseconds,
    TimeWithNanoseconds,
}

pub fn match_var_format(format_str: &str,file_type: &ReadstatFileType) -> Option<ReadStatVarFormatClass> {
    match file_type {
        ReadstatFileType::Sas7bdat => unsafe {
            match_var_format_sas(format_str)
        },
        ReadstatFileType::Dta => unsafe {
            match_var_format_stata(format_str)
        },
        // Add other file types here
    }
}



fn match_var_format_stata(format_str: &str) -> Option<ReadStatVarFormatClass> {
    // Compile regex patterns for different Stata format types
    
    // Compile regex patterns for different Stata format types
    // Date formats: %td, %d followed by optional display format
    let date_regex = Regex::new(r"(?i)^%t?d(?:[a-z0-9:._-]*)$").unwrap();
    
    // Time formats: %th followed by optional display format
    let time_regex = Regex::new(r"(?i)^%t?h(?:[a-z0-9:._-]*)$").unwrap();
    
    // Basic datetime formats: %tc followed by optional display format
    let datetime_regex = Regex::new(r"(?i)^%t?c(?:[a-z0-9:._-]*)$").unwrap();
    
    // Datetime with milliseconds precision: %tC followed by optional display format
    let datetime_ms_regex = Regex::new(r"(?i)^%t?C(?:[a-z0-9:._-]*)$").unwrap();
    
    // Datetime with microseconds: %tu followed by optional display format
    let datetime_us_regex = Regex::new(r"(?i)^%t?u(?:[a-z0-9:._-]*)$").unwrap();
    
    // Datetime with nanoseconds: %tN followed by optional display format
    let datetime_ns_regex = Regex::new(r"(?i)^%t?N(?:[a-z0-9:._-]*)$").unwrap();


    // Check the format string against each regex pattern
    if datetime_ns_regex.is_match(format_str) {
        Some(ReadStatVarFormatClass::DateTimeWithNanoseconds)
    } else if datetime_us_regex.is_match(format_str) {
        Some(ReadStatVarFormatClass::DateTimeWithMicroseconds)
    } else if datetime_ms_regex.is_match(format_str) {
        Some(ReadStatVarFormatClass::DateTimeWithMilliseconds)
    } else if datetime_regex.is_match(format_str) {
        Some(ReadStatVarFormatClass::DateTime)
    } else if date_regex.is_match(format_str) {
        Some(ReadStatVarFormatClass::Date)
    } else if time_regex.is_match(format_str) {
        Some(ReadStatVarFormatClass::Time)
    } else {
        None // Format not recognized
    }
}

fn match_var_format_sas(format_str: &str) -> Option<ReadStatVarFormatClass> {
    
    // Compile regex patterns for different format types
    
    // Date formats: DATE, MMDDYY, DDMMYY, YYMMDD, etc.
    let date_regex = Regex::new(r"(?i)^(DATE|MMDDYY|DDMMYY|YYMMDD|MONYY|MONNAME|WEEKDATE|WEEKDATX|WORDDATE|WORDDATX|JULDAY|JULIAN|QTR|QTRR|YEAR|EURDFDE|NENGO)[0-9]*\.?[0-9]*$").unwrap();
    
    // Basic time formats without fractional seconds
    let time_regex = Regex::new(r"(?i)^(TIME|HHMM|HOUR|MMSS|TOD)[0-9]*\.?[0-9]*$").unwrap();
    
    // Time formats with milliseconds (usually .3 precision)
    let time_ms_regex = Regex::new(r"(?i)^(TIME|HHMM|TOD)[0-9]*\.3$").unwrap();
    
    // Time formats with microseconds (usually .6 precision)
    let time_us_regex = Regex::new(r"(?i)^(TIME|HHMM|TOD)[0-9]*\.6$").unwrap();
    
    // Time formats with nanoseconds (usually .9 precision)
    let time_ns_regex = Regex::new(r"(?i)^(TIME|HHMM|TOD)[0-9]*\.9$").unwrap();
    
    // Basic datetime formats without fractional seconds
    let datetime_regex = Regex::new(r"(?i)^(DATETIME|E8601DT|B8601DT|MDYAMPM|DTDATE|DTMONYY)[0-9]*\.?[0-9]*$").unwrap();
    
    // Datetime formats with milliseconds (usually .3 precision)
    let datetime_ms_regex = Regex::new(r"(?i)^(DATETIME|E8601DT|B8601DT)[0-9]*\.3$").unwrap();
    
    // Datetime formats with microseconds (usually .6 precision)
    let datetime_us_regex = Regex::new(r"(?i)^(DATETIME|E8601DT|B8601DT)[0-9]*\.6$").unwrap();
    
    // Datetime formats with nanoseconds (usually .9 precision)
    let datetime_ns_regex = Regex::new(r"(?i)^(DATETIME|E8601DT|B8601DT)[0-9]*\.9$").unwrap();
    
    // Special cases for DATETIMExx formats with implied precision
    let datetime_ms_implied_regex = Regex::new(r"(?i)^DATETIME(21|22)[0-9]*\.?[0-9]*$").unwrap();
    let datetime_us_implied_regex = Regex::new(r"(?i)^DATETIME(23|24)[0-9]*\.?[0-9]*$").unwrap();
    let datetime_ns_implied_regex = Regex::new(r"(?i)^DATETIME(25|26)[0-9]*\.?[0-9]*$").unwrap();

    // Check the format string against each regex pattern - order is important
    // Start with the most specific patterns first
    if datetime_ns_implied_regex.is_match(format_str) {
        // DATETIME25 and DATETIME26 are always nanosecond precision
        Some(ReadStatVarFormatClass::DateTimeWithNanoseconds)
    } else if datetime_us_implied_regex.is_match(format_str) {
        // DATETIME23 and DATETIME24 are always microsecond precision
        Some(ReadStatVarFormatClass::DateTimeWithMicroseconds)
    } else if datetime_ms_implied_regex.is_match(format_str) {
        // DATETIME21 and DATETIME22 are always millisecond precision
        Some(ReadStatVarFormatClass::DateTimeWithMilliseconds)
    } else if datetime_ns_regex.is_match(format_str) {
        Some(ReadStatVarFormatClass::DateTimeWithNanoseconds)
    } else if datetime_us_regex.is_match(format_str) {
        Some(ReadStatVarFormatClass::DateTimeWithMicroseconds)
    } else if datetime_ms_regex.is_match(format_str) {
        Some(ReadStatVarFormatClass::DateTimeWithMilliseconds)
    } else if time_ns_regex.is_match(format_str) {
        Some(ReadStatVarFormatClass::TimeWithNanoseconds)
    } else if time_us_regex.is_match(format_str) {
        Some(ReadStatVarFormatClass::TimeWithMicroseconds)
    } else if time_ms_regex.is_match(format_str) {
        Some(ReadStatVarFormatClass::TimeWithMilliseconds)
    } else if datetime_regex.is_match(format_str) {
        Some(ReadStatVarFormatClass::DateTime)
    } else if date_regex.is_match(format_str) {
        Some(ReadStatVarFormatClass::Date)
    } else if time_regex.is_match(format_str) {
        Some(ReadStatVarFormatClass::Time)
    } else {
        None // Format not recognized
    }
}
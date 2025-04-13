use lazy_static::lazy_static;
use log::debug;
use regex::Regex;

use crate::rs_var::ReadStatVarFormatClass;

pub fn match_var_format(
    v: &str,
    extension: &str
) -> Option<ReadStatVarFormatClass> {

    if extension == "sas7bdat" {
        match match_var_format_sas(&v) {
            Some(result) => Some(result),
            None => None
        }
    }
    else if extension == "dta" {
        match match_var_format_stata(&v) {
            Some(result) => Some(result),
            None => None
        }
    }
    else {
        None
    }
}




fn match_var_format_sas(
    format_str: &str
) -> Option<ReadStatVarFormatClass> {

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


fn match_var_format_stata(format_str: &str) -> Option<ReadStatVarFormatClass> {
    // --- Regex Definitions (inside function scope as requested) ---
    // WARNING: Compiling Regexes on every call is inefficient. Consider using once_cell or lazy_static.

    // 1. Specific Time-Only Display Formats (Check FIRST!)
    let time_hms_sub_regex = Regex::new(r"(?i)^%t[ch]\s*[Hh][Hh]:[Mm][Mm]:[Ss][Ss](?:\.[Ss]{1,9})?$").unwrap();
    let time_hms_regex = Regex::new(r"(?i)^%t[ch]\s*[Hh][Hh]:[Mm][Mm]:[Ss][Ss]$").unwrap();
    let time_hm_regex = Regex::new(r"(?i)^%t[ch]\s*[Hh][Hh]:[Mm][Mm]$").unwrap();

    // 2. High Precision DateTime (Potentially less standard)
    let datetime_ns_regex = Regex::new(r"(?i)^%t?N").unwrap(); // e.g., %tN...
    let datetime_us_regex = Regex::new(r"(?i)^%t?u").unwrap(); // e.g., %tU...

    // 3. DateTime with Milliseconds (%tC base type)
    let datetime_milli_base_regex = Regex::new(r"(?i)^%t?C$").unwrap(); // Exact %tC or %C
    // Matches %tC... or %C... *with display specifiers* (that are not time-only)
    let datetime_milli_display_regex = Regex::new(r"(?i)^%t?C.").unwrap();

    // 4. Standard DateTime (%tc base type)
    let datetime_base_regex = Regex::new(r"(?i)^%t?c$").unwrap(); // Exact %tc or %c
     // Matches %tc... or %c... *with display specifiers* (that are not time-only)
    let datetime_display_regex = Regex::new(r"(?i)^%t?c.").unwrap();

    // 5. Date Formats (includes %td, %d, and periods)
    let date_td_base_regex = Regex::new(r"(?i)^%t?d$").unwrap(); // Exact %td or %d
    // Matches %td... or %d... *with display specifiers*
    let date_td_display_regex = Regex::new(r"(?i)^%t?d.").unwrap();
    // Matches period formats like %tw, %tm, %tq, %th, %ty, %tb (with or without display specifiers)
    let date_periods_regex = Regex::new(r"(?i)^%t[wmtqhyb]").unwrap();
    // Specific examples from pyreadstat/original code
    let date_format_regex = Regex::new(r"(?i)^%tdD_m_Y$").unwrap();
    let date_iso_regex = Regex::new(r"(?i)^%tdCCYY-NN-DD$").unwrap();

    // Add other date formats from your original code if needed, e.g.:
    // let dmy_regex = Regex::new(r"(?i)^%(?:dmy|dmys)(?:[a-z0-9:._\s-]*)$").unwrap();
    // let mdy_regex = Regex::new(r"(?i)^%(?:mdy|mdys)(?:[a-z0-9:._\s-]*)$").unwrap();
    // Be careful they don't incorrectly match other types.

    // --- Check the format string against each regex pattern (ORDER MATTERS!) ---

    // 1. Check for TIME FIRST
    if time_hms_sub_regex.is_match(format_str)
        || time_hms_regex.is_match(format_str)
        || time_hm_regex.is_match(format_str)
    {
        Some(ReadStatVarFormatClass::Time)
    }
    // 2. Then High Precision DateTime
    else if datetime_ns_regex.is_match(format_str) {
        Some(ReadStatVarFormatClass::DateTimeWithNanoseconds)
    } else if datetime_us_regex.is_match(format_str) {
        Some(ReadStatVarFormatClass::DateTimeWithMicroseconds)
    }
    // 3. Then Millisecond DateTime (%tC)
    else if datetime_milli_base_regex.is_match(format_str) || datetime_milli_display_regex.is_match(format_str) {
        // Time-only versions were already caught by the first check
        Some(ReadStatVarFormatClass::DateTimeWithMilliseconds)
    }
    // 4. Then Standard DateTime (%tc)
    else if datetime_base_regex.is_match(format_str) || datetime_display_regex.is_match(format_str) {
         // Time-only versions were already caught by the first check
        Some(ReadStatVarFormatClass::DateTime)
    }
    // 5. Then Date formats
    else if date_td_base_regex.is_match(format_str)
        || date_td_display_regex.is_match(format_str)
        || date_periods_regex.is_match(format_str)
        || date_format_regex.is_match(format_str) // pyreadstat specific example
        || date_iso_regex.is_match(format_str)   // pyreadstat specific example
        // || dmy_regex.is_match(format_str) // Add others here if needed
        // || mdy_regex.is_match(format_str)
    {
        Some(ReadStatVarFormatClass::Date)
    }
    // --- NO MATCH ---
    else {
        None // Format not recognized as a known date/time type
    }
}
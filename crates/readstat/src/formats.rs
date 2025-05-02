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
    else if extension == "sav" {
        match match_var_format_sav(&v) {
            Some(result) => Some(result),
            None => None
        }
    }
    else {
        None
    }
}




fn match_var_format_sas(format_str: &str) -> Option<ReadStatVarFormatClass> {
    // Convert to uppercase for matching
    let format_upper = format_str.to_uppercase();
    
    // Define the format lists like pyreadstat
    let sas_date_formats = [
        "WEEKDATE", "MMDDYY", "DDMMYY", "YYMMDD", "DATE", "DATE9", "YYMMDD10", 
        "DDMMYYB", "DDMMYYB10", "DDMMYYC", "DDMMYYC10", "DDMMYYD", "DDMMYYD10",
        "DDMMYYN6", "DDMMYYN8", "DDMMYYP", "DDMMYYP10", "DDMMYYS", "DDMMYYS10",
        "MMDDYYB", "MMDDYYB10", "MMDDYYC", "MMDDYYC10", "MMDDYYD", "MMDDYYD10",
        "MMDDYYN6", "MMDDYYN8", "MMDDYYP", "MMDDYYP10", "MMDDYYS", "MMDDYYS10",
        "WEEKDATX", "DTDATE", "WORDDATE", "WORDDATX", "JULDAY", "JULIAN",
        "IS8601DA", "E8601DA", "B8601DA", "EURDFDE", "NENGO", "MONYY", "QTR", "QTRR", "YEAR",
        "YYMMDDB", "YYMMDDD", "YYMMDDN", "YYMMDDP", "YYMMDDS"
    ];
    
    let sas_datetime_formats = [
        "DATETIME", "DATETIME18", "DATETIME19", "DATETIME20", "DATETIME21", "DATETIME22",
        "DATETIME23", "DATETIME24", "DATETIME25", "DATETIME26",
        "E8601DT", "DATEAMPM", "MDYAMPM", "DTMONYY", "IS8601DT", "B8601DT", "B8601DN"
    ];
    
    let sas_time_formats = [
        "TIME", "HHMM", "TIME20.3", "TIME20", "TIME5", "TOD", "HOUR", "MMSS",
        "TIMEAMPM", "IS8601TM", "E8601TM", "B8601TM"
    ];
    
    // Strip any width and precision specs (numbers and decimal points)
    // This creates a base format name for matching
    let base_format = format_upper
        .chars()
        .take_while(|&c| c.is_alphabetic() || c == '8' || c == '6' || c == '0' || c == '1')
        .collect::<String>();
    
    // Try to match the base format to our lists
    for fmt in &sas_time_formats {
        if base_format == *fmt || format_upper.starts_with(fmt) {
            return Some(ReadStatVarFormatClass::Time);
        }
    }
    
    for fmt in &sas_datetime_formats {
        if base_format == *fmt || format_upper.starts_with(fmt) {
            return Some(ReadStatVarFormatClass::DateTime);
        }
    }
    
    for fmt in &sas_date_formats {
        if base_format == *fmt || format_upper.starts_with(fmt) {
            return Some(ReadStatVarFormatClass::Date);
        }
    }
    
    // Special case for precision handling (if needed)
    if format_upper.contains(".3") && (
        format_upper.starts_with("TIME") || 
        format_upper.starts_with("HHMM") || 
        format_upper.starts_with("TOD")
    ) {
        return Some(ReadStatVarFormatClass::Time);
    }
    
    // No match found
    None
}

fn match_var_format_stata(format_str: &str) -> Option<ReadStatVarFormatClass> {
    // Convert to lowercase for case-insensitive matching
    let format_lower = format_str.to_lowercase();
    
    // 1. Check for TIME formats first (most specific)
    if format_lower.contains("hh:mm:ss") || format_lower.contains("hh:mm") {
        return Some(ReadStatVarFormatClass::Time);
    }
    
    // 2. Check for DATETIME formats
    if format_lower.starts_with("%tc") || format_lower.starts_with("%c") ||
       format_lower.starts_with("%tn") || format_lower.starts_with("%n") ||
       format_lower.starts_with("%tu") || format_lower.starts_with("%u") {
        return Some(ReadStatVarFormatClass::DateTime);
    }
    
    // 3. Check for DATE formats
    if format_lower.starts_with("%td") || format_lower.starts_with("%d") ||
       format_lower.starts_with("%tw") || format_lower.starts_with("%tm") || 
       format_lower.starts_with("%tq") || format_lower.starts_with("%th") || 
       format_lower.starts_with("%ty") || format_lower.starts_with("%tb") ||
       format_lower == "%tdd_m_y" || format_lower == "%tdccyy-nn-dd" ||
       format_lower == "%d"{
        return Some(ReadStatVarFormatClass::Date);
    }
    
    // No match found
    None
}


fn match_var_format_sav(format_str: &str) -> Option<ReadStatVarFormatClass> {
    
    let spss_datetime_formats = ["DATETIME", "DATETIME8", "DATETIME17", "DATETIME20", "DATETIME23.2","YMDHMS16","YMDHMS19","YMDHMS19.2", "YMDHMS20"];
    let spss_date_formats = ["DATE","DATE8","DATE11", "DATE12", "ADATE","ADATE8", "ADATE10", "EDATE", "EDATE8","EDATE10", "JDATE", "JDATE5", "JDATE7", "SDATE", "SDATE8", "SDATE10"];
    let spss_time_formats = ["TIME", "DTIME", "TIME8", "TIME5", "TIME11.2"];

    if spss_date_formats.contains(&format_str) {
        Some(ReadStatVarFormatClass::Date)
    } else if spss_time_formats.contains(&format_str) {
        Some(ReadStatVarFormatClass::Time)
    } else if spss_datetime_formats.contains(&format_str) {
        Some(ReadStatVarFormatClass::DateTime)
    } else {
        None // Format not recognized as a known date/time type
    }
}
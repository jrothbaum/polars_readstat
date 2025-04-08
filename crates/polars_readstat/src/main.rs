use log::{debug, info, warn, error};
use env_logger::Builder;
use polars_arrow::types::Index;
use std::{env, io::Write};
use readstat;

/*
mod error;
mod ffi;
mod types;
mod common;
mod formats;
mod read;

 */


fn main() {
    unsafe {
        //  env::set_var("POLARS_MAX_THREADS", "1");
    }
    //  env_logger::init();
    Builder::from_default_env()
        .format(|buf, record| {
            writeln!(buf, "[{}] {}", 
                record.level(),
                record.args()
            )
        })
        .init();

    
    
    let _ = read::read_file_parallel(&std::path::PathBuf::from("/home/jrothbaum/python/polars_readstat/crates/polars_readstat/tests/data/sample_pyreadstat.dta"),
                             read::ReadstatFileType::Dta,
                             None // Some(2)
                            );

    /*
    let _ = read::read_file_parallel(&std::path::PathBuf::from("/home/jrothbaum/python/readstat-rs/crates/readstat-tests/tests/data/all_types.sas7bdat"),
                            read::ReadstatFileType::Sas7bdat,
                            None
                           );
*/    
    debug!("FINISHED");
    
                            
    //  let _ = test_parser();
}


/*
// Function to test the parser
fn test_parser() {
    use crate::formats::{ReadStatVarFormatClass,match_var_format};

    let test_cases = vec![
        // Date formats
        ("DATE", ReadStatVarFormatClass::Date),
        ("DATE9.", ReadStatVarFormatClass::Date),
        ("MMDDYY10.", ReadStatVarFormatClass::Date),
        
        // Time formats
        ("TIME", ReadStatVarFormatClass::Time),
        ("TIME8.", ReadStatVarFormatClass::Time),
        ("HHMM5.", ReadStatVarFormatClass::Time),
        
        // Time with precision
        ("TIME.3", ReadStatVarFormatClass::TimeWithMilliseconds),
        ("HHMM.3", ReadStatVarFormatClass::TimeWithMilliseconds),
        ("TIME.6", ReadStatVarFormatClass::TimeWithMicroseconds),
        ("TOD.6", ReadStatVarFormatClass::TimeWithMicroseconds),
        ("TIME.9", ReadStatVarFormatClass::TimeWithNanoseconds),
        ("TOD.9", ReadStatVarFormatClass::TimeWithNanoseconds),
        
        // DateTime formats
        ("DATETIME", ReadStatVarFormatClass::DateTime),
        ("DATETIME12", ReadStatVarFormatClass::DateTime),
        ("DATETIME19.", ReadStatVarFormatClass::DateTime),
        ("DATETIME21.", ReadStatVarFormatClass::DateTime),
        ("DATETIME22.", ReadStatVarFormatClass::DateTime),
        ("DATETIME23.", ReadStatVarFormatClass::DateTime),
        ("DATETIME24.", ReadStatVarFormatClass::DateTime),
        ("DATETIME25.", ReadStatVarFormatClass::DateTime),
        ("DATETIME26.", ReadStatVarFormatClass::DateTime),
        

        // DateTime with precision
        ("DATETIME.3", ReadStatVarFormatClass::DateTimeWithMilliseconds),
        ("E8601DT.3", ReadStatVarFormatClass::DateTimeWithMilliseconds),
        ("DATETIME.6", ReadStatVarFormatClass::DateTimeWithMicroseconds),
        ("E8601DT.6", ReadStatVarFormatClass::DateTimeWithMicroseconds),
        ("DATETIME.9", ReadStatVarFormatClass::DateTimeWithNanoseconds),
        ("E8601DT.9", ReadStatVarFormatClass::DateTimeWithNanoseconds),
    ];
    
    for (format_str, expected) in test_cases {
        match match_var_format(format_str,&read::ReadstatFileType::Sas7bdat) {
            Some(result) => {
                println!("Format '{}' parsed as {:?}", format_str, result);
                // In a real test, you would assert equality here
            },
            None => println!("Format '{}' not recognized", format_str),
        }
    }
}
 */
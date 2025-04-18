#![allow(non_camel_case_types)]
pub use err::ReadStatError;
pub use rs_data::ReadStatData;
pub use rs_metadata::{ReadStatCompress, ReadStatEndian, ReadStatMetadata, ReadStatVarMetadata};
pub use rs_path::ReadStatPath;
pub use rs_var::{ReadStatVar, ReadStatVarFormatClass, ReadStatVarType, ReadStatVarTypeClass};

mod cb;
mod common;
mod err;
mod formats;
mod rs_data;
mod rs_metadata;
mod rs_parser;
mod rs_path;
mod rs_var;
//  mod rs_write;

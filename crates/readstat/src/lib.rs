#![allow(non_camel_case_types)]
pub use err::ReadStatError;
pub use rs_data::ReadStatData;
pub use rs_metadata::{
    ReadStatCompress,
    ReadStatEndian,
    ReadStatMetadata,
    ReadStatVarMetadata,
    LabelValue
};
pub use rs_path::ReadStatPath;
pub use rs_var::{ReadStatVar, ReadStatVarFormatClass, ReadStatVarType, ReadStatVarTypeClass};
pub use rs_parser::ReadStatParser;
pub use stream::ReadStatStreamer;

mod cb;
mod common;
mod err;
//  mod series_builder;
mod stream;
mod formats;
mod rs_data;
mod rs_metadata;
mod rs_parser;
mod rs_path;
mod rs_var;
//  mod rs_write;

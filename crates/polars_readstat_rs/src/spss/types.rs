#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endian {
    Little,
    Big,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VarType {
    Numeric,
    Str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatClass {
    Date,
    DateTime,
    Time,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValueLabelKey {
    Double(f64),
    Str(String),
}

#[derive(Debug, Clone)]
pub struct Header {
    pub version: u32,
    pub endian: Endian,
    pub compression: i32,
    pub nominal_case_size: i32,
    pub row_count: i32,
    pub bias: f64,
    pub data_label: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Variable {
    pub name: String,
    pub short_name: String,
    pub var_type: VarType,
    pub width: usize,      // number of 8-byte segments
    pub string_len: usize, // declared string length in bytes (0 for numeric)
    pub format_type: u8,
    pub format_class: Option<FormatClass>,
    pub label: Option<String>,
    pub value_label: Option<String>,
    pub offset: usize, // segment offset within row
    pub missing_range: bool,
    pub missing_doubles: Vec<f64>,
    pub missing_double_bits: Vec<u64>,
    pub missing_strings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Metadata {
    pub variables: Vec<Variable>,
    pub row_count: u64,
    pub data_offset: Option<u64>,
    pub encoding: &'static encoding_rs::Encoding,
    pub value_labels: Vec<ValueLabel>,
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            variables: Vec::new(),
            row_count: 0,
            data_offset: None,
            encoding: encoding_rs::WINDOWS_1252,
            value_labels: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ValueLabel {
    pub name: String,
    pub mapping: Vec<(ValueLabelKey, String)>,
}

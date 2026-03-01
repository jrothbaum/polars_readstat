use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endian {
    Little,
    Big,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericType {
    Byte,
    Int,
    Long,
    Float,
    Double,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VarType {
    Numeric(NumericType),
    Str(u16),
    StrL,
}

#[derive(Debug, Clone)]
pub struct Header {
    pub version: u16,
    pub endian: Endian,
    pub nvars: u32,
    pub nobs: u64,
    pub data_label: Option<String>,
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Variable {
    pub name: String,
    pub var_type: VarType,
    pub format: Option<String>,
    pub label: Option<String>,
    pub value_label_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Metadata {
    pub variables: Vec<Variable>,
    pub value_labels: Vec<ValueLabel>,
    pub sort_order: Vec<u32>,
    pub byte_order: Endian,
    pub row_count: u64,
    pub data_label: Option<String>,
    pub timestamp: Option<String>,
    pub storage_widths: Vec<u16>,
    pub formats: Vec<String>,
    pub variable_labels: Vec<String>,
    pub data_offset: Option<u64>,
    pub strls_offset: Option<u64>,
    pub value_labels_offset: Option<u64>,
    pub encoding: &'static encoding_rs::Encoding,
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            variables: Vec::new(),
            value_labels: Vec::new(),
            sort_order: Vec::new(),
            byte_order: Endian::Little,
            row_count: 0,
            data_label: None,
            timestamp: None,
            storage_widths: Vec::new(),
            formats: Vec::new(),
            variable_labels: Vec::new(),
            data_offset: None,
            strls_offset: None,
            value_labels_offset: None,
            encoding: encoding_rs::WINDOWS_1252,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ValueLabel {
    pub name: String,
    pub mapping: Arc<Vec<(ValueLabelKey, String)>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValueLabelKey {
    Integer(i32),
    Double(f64),
    Str(String),
}

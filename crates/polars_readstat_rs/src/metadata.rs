use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Metadata {
    pub file_info: MetadataFileInfo,
    pub column_info: Vec<MetadataColumnInfo>,
}

#[derive(Debug, Clone)]
pub struct MetadataFileInfo {
    pub n_rows: u64,
    pub n_cols: u32,
    pub size: Option<u64>,
    pub page_size: Option<u32>,
    pub page_count: Option<u32>,
    pub header_length: Option<u32>,
    pub row_length: Option<u32>,
    pub compression: Option<String>,  // "None", "RLE", "RDC", etc.
    
    // File metadata
    pub name: Option<String>,
    pub file_label: Option<String>,
    pub encoding: Option<String>,
    pub file_type: Option<String>,
    pub sas_release: Option<String>,
    pub sas_server_type: Option<String>,
    pub os_name: Option<String>,
    pub creator: Option<String>,
    pub creator_proc: Option<String>,
    
    // Timestamps
    pub created_date: Option<i64>,
    pub modified_date: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct MetadataColumnInfo {
    pub name: String,
    pub index: u32,
    
    // Type information
    pub type_class: Option<String>,
    
    // SAS-specific formatting
    pub format: Option<String>,      // SAS format like "DATE9.", "DATETIME20."
    pub label: Option<String>,       // Descriptive label
    
    // Additional metadata
    pub length: Option<u32>,
    pub value_labels: Option<HashMap<String, String>>

}
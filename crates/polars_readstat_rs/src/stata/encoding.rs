use encoding_rs::Encoding;

pub fn default_encoding(ds_format: u16) -> &'static Encoding {
    if ds_format >= 118 {
        encoding_rs::UTF_8
    } else {
        encoding_rs::WINDOWS_1252
    }
}

pub fn decode_string(bytes: &[u8], encoding: &'static Encoding) -> String {
    let (decoded, _, _had_errors) = encoding.decode(bytes);
    decoded.into_owned()
}

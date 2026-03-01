use encoding_rs::Encoding;

/// Map SAS encoding byte to encoding name (matches C++ implementation)
pub fn get_encoding_name(encoding_byte: u8) -> &'static str {
    const DEFAULT: &str = "WINDOWS-1252";

    match encoding_byte {
        20 => "UTF-8",
        28 => "US-ASCII",
        29 => "ISO-8859-1",
        30 => "ISO-8859-2",
        31 => "ISO-8859-3",
        32 => "ISO-8859-4",
        33 => "ISO-8859-5",
        34 => "ISO-8859-6",
        35 => "ISO-8859-7",
        36 => "ISO-8859-8",
        37 => "ISO-8859-9",
        39 => "ISO-8859-11",
        40 => "ISO-8859-15",
        // Code pages
        41 => "CP437",
        42 => "CP850",
        43 => "CP852",
        44 => "CP857",
        45 => "CP858",
        46 => "CP862",
        47 => "CP864",
        48 => "CP865",
        49 => "CP866",
        50 => "CP869",
        51 => "CP874",
        52 => "CP921",
        53 => "CP922",
        54 => "CP1129",
        55 => "CP720",
        56 => "CP737",
        57 => "CP775",
        58 => "CP860",
        59 => "CP863",
        60 => "WINDOWS-1250",
        61 => "WINDOWS-1251",
        62 => "WINDOWS-1252",
        63 => "WINDOWS-1253",
        64 => "WINDOWS-1254",
        65 => "WINDOWS-1255",
        66 => "WINDOWS-1256",
        67 => "WINDOWS-1257",
        68 => "WINDOWS-1258",
        69 => "MACROMAN",
        70 => "MACARABIC",
        71 => "MACHEBREW",
        72 => "MACGREEK",
        73 => "MACTHAI",
        75 => "MACTURKISH",
        76 => "MACUKRAINE",
        // Asian encodings
        118 => "CP950", // Traditional Chinese
        119 => "EUC-TW",
        123 => "BIG5-HKSCS",
        125 => "GB18030", // Simplified Chinese
        126 => "CP936",   // Simplified Chinese
        128 => "CP1381",
        134 => "EUC-JP", // Japanese
        136 => "CP949",
        137 => "CP942",
        138 => "CP932",
        140 => "EUC-KR",
        141 => "CP949",
        142 => "CP949",
        163 => "MACICELAND",
        167 => "ISO-2022-JP",
        168 => "ISO-2022-KR",
        169 => "ISO-2022-CN",
        172 => "ISO-2022-CN-EXT",
        205 => "GB18030",
        227 => "ISO-8859-14",
        242 => "ISO-8859-13",
        245 => "MACCROATIAN",
        246 => "MACCYRILLIC",
        247 => "MACROMANIA",
        248 => "SHIFT_JISX0213",
        _ => DEFAULT,
    }
}

/// Get encoding_rs Encoding from SAS encoding byte
pub fn get_encoding(encoding_byte: u8) -> &'static Encoding {
    let name = get_encoding_name(encoding_byte);

    // Map encoding names to encoding_rs encodings
    match name {
        "UTF-8" => encoding_rs::UTF_8,
        "US-ASCII" => encoding_rs::WINDOWS_1252, // ASCII is subset of Windows-1252
        "ISO-8859-1" => encoding_rs::WINDOWS_1252, // Latin-1 is subset of Windows-1252
        "ISO-8859-2" => encoding_rs::ISO_8859_2,
        "ISO-8859-3" => encoding_rs::ISO_8859_3,
        "ISO-8859-4" => encoding_rs::ISO_8859_4,
        "ISO-8859-5" => encoding_rs::ISO_8859_5,
        "ISO-8859-6" => encoding_rs::ISO_8859_6,
        "ISO-8859-7" => encoding_rs::ISO_8859_7,
        "ISO-8859-8" => encoding_rs::ISO_8859_8,
        "ISO-8859-9" => encoding_rs::WINDOWS_1254, // ISO-8859-9 is similar to Windows-1254
        "ISO-8859-11" => encoding_rs::WINDOWS_874, // ISO-8859-11 is Thai, similar to Windows-874
        "ISO-8859-13" => encoding_rs::ISO_8859_13,
        "ISO-8859-14" => encoding_rs::ISO_8859_14,
        "ISO-8859-15" => encoding_rs::ISO_8859_15,
        "CP874" => encoding_rs::WINDOWS_874,
        "CP866" => encoding_rs::IBM866,
        "WINDOWS-1250" => encoding_rs::WINDOWS_1250,
        "WINDOWS-1251" => encoding_rs::WINDOWS_1251,
        "WINDOWS-1252" => encoding_rs::WINDOWS_1252,
        "WINDOWS-1253" => encoding_rs::WINDOWS_1253,
        "WINDOWS-1254" => encoding_rs::WINDOWS_1254,
        "WINDOWS-1255" => encoding_rs::WINDOWS_1255,
        "WINDOWS-1256" => encoding_rs::WINDOWS_1256,
        "WINDOWS-1257" => encoding_rs::WINDOWS_1257,
        "WINDOWS-1258" => encoding_rs::WINDOWS_1258,
        // Asian encodings
        "CP950" => encoding_rs::BIG5,      // Traditional Chinese
        "BIG5-HKSCS" => encoding_rs::BIG5, // No direct support; use BIG5
        "EUC-TW" => encoding_rs::BIG5,     // No direct support; use BIG5
        "GB18030" => encoding_rs::GB18030,
        "CP936" => encoding_rs::GBK,      // Simplified Chinese
        "CP1381" => encoding_rs::GB18030, // Closest available
        "EUC-JP" => encoding_rs::EUC_JP,
        "CP932" => encoding_rs::SHIFT_JIS,
        "CP942" => encoding_rs::SHIFT_JIS,
        "SHIFT_JISX0213" => encoding_rs::SHIFT_JIS,
        "CP949" => encoding_rs::EUC_KR, // Closest available
        "EUC-KR" => encoding_rs::EUC_KR,
        "ISO-2022-JP" => encoding_rs::ISO_2022_JP,
        "ISO-2022-KR" => encoding_rs::EUC_KR, // No direct support; closest available
        "ISO-2022-CN" => encoding_rs::GB18030, // No direct support; closest available
        "ISO-2022-CN-EXT" => encoding_rs::GB18030, // No direct support; closest available
        // Mac encodings
        "MACROMAN" => encoding_rs::MACINTOSH,
        "MACARABIC" => encoding_rs::MACINTOSH,
        "MACHEBREW" => encoding_rs::MACINTOSH,
        "MACGREEK" => encoding_rs::MACINTOSH,
        "MACTHAI" => encoding_rs::MACINTOSH,
        "MACTURKISH" => encoding_rs::MACINTOSH,
        "MACUKRAINE" => encoding_rs::MACINTOSH,
        "MACICELAND" => encoding_rs::MACINTOSH,
        "MACCROATIAN" => encoding_rs::MACINTOSH,
        "MACROMANIA" => encoding_rs::MACINTOSH,
        "MACCYRILLIC" => encoding_rs::X_MAC_CYRILLIC,
        _ => encoding_rs::WINDOWS_1252, // Default fallback
    }
}

/// Decode bytes to UTF-8 string using the specified encoding byte.
/// ISO-8859-1 needs special handling because encoding_rs maps it to Windows-1252.
pub fn decode_string(bytes: &[u8], encoding_byte: u8, encoding: &'static Encoding) -> String {
    if encoding_byte == 29 {
        // ISO-8859-1 is a 1:1 mapping of bytes to Unicode code points U+0000..U+00FF.
        return bytes.iter().map(|&b| b as char).collect();
    }

    let (decoded, _, _had_errors) = encoding.decode(bytes);
    decoded.into_owned()
}

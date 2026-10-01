use polars_readstat_rs::{read_sas7bcat, CatalogKey};
use std::path::PathBuf;

#[test]
fn test_read_catalog_small() {
    let path = PathBuf::from("tests/sas/data/too_big/sas_format/formats.sas7bcat");
    if !path.exists() {
        return;
    }

    let catalog = read_sas7bcat(&path).expect("catalog should parse");
    assert_eq!(catalog.len(), 10);

    let sexfmt = catalog.get("SEXFMT").expect("SEXFMT format present");
    assert_eq!(
        sexfmt,
        &vec![
            (CatalogKey::Numeric(1.0), "Male".to_string()),
            (CatalogKey::Numeric(2.0), "Female".to_string()),
        ]
    );

    let agefmt = catalog.get("AGEFMT").expect("AGEFMT format present");
    assert!(agefmt.contains(&(CatalogKey::Numeric(89.0), "89 +".to_string())));
}

#[test]
fn test_read_catalog_large() {
    let path = PathBuf::from("tests/sas/data/data_gov/formats.sas7bcat");
    if !path.exists() {
        return;
    }

    let catalog = read_sas7bcat(&path).expect("catalog should parse");
    assert_eq!(catalog.len(), 189);

    let total_labels: usize = catalog.values().map(|v| v.len()).sum();
    assert_eq!(total_labels, 834); // matches pyreadstat exactly, see compare_catalog_to_python.py

    let f_p395f = catalog.get("F_P395F").expect("F_P395F format present");
    assert!(f_p395f.contains(&(
        CatalogKey::Numeric(0.0),
        "Not imputed (original data)".to_string()
    )));

    // A format that assigns a label to missing/tagged-missing values (`.`, `.A`-`.Z`)
    // should surface it as CatalogKey::Missing(None) rather than silently dropping it.
    let p445f = catalog.get("P445F").expect("P445F format present");
    assert!(p445f.contains(&(CatalogKey::Missing(None), "Valid Skip".to_string())));
}

/// haven's tagged-na fixture: format XFMT labels `.A` "Apple" and `.Z` "Zebra".
/// pyreadstat reports `{'A': 'Apple', 'Z': 'Zebra'}`.
#[test]
fn test_read_catalog_tagged_missing() {
    let path = PathBuf::from("tests/sas/data/haven/tagged-na.sas7bcat");
    let catalog = read_sas7bcat(&path).expect("catalog should parse");
    assert_eq!(
        catalog.get("XFMT").expect("XFMT format present"),
        &vec![
            (CatalogKey::Missing(Some('A')), "Apple".to_string()),
            (CatalogKey::Missing(Some('Z')), "Zebra".to_string()),
        ]
    );
}

#[test]
fn test_read_catalog_haven_formats() {
    let path = PathBuf::from("tests/sas/data/haven/formats.sas7bcat");
    let catalog = read_sas7bcat(&path).expect("catalog should parse");
    assert_eq!(catalog.len(), 2);
}

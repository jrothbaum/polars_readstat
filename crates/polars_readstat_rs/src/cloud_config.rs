//! Builds the [`ObjectStore`] for a cloud URL with the same credential and region
//! resolution Polars uses for `pl.scan_parquet("s3://…")`, so the two behave alike.
//!
//! For `s3://`, lowest to highest precedence:
//! 1. `AWS_*` environment variables (as `AmazonS3Builder::from_env`),
//! 2. `~/.aws/config` (region) and `~/.aws/credentials` (access key, secret, session
//!    token) — read exactly as polars-io does: those two fixed paths, and the *first*
//!    occurrence of each key anywhere in the file, regardless of profile
//!    (`AWS_PROFILE`, `AWS_CONFIG_FILE` and `AWS_SHARED_CREDENTIALS_FILE` are not
//!    consulted). Keys already set by the environment are left alone,
//! 3. the explicit `storage_options` passed by the caller.
//!
//! If no region results and no custom endpoint is set, the bucket's region is looked
//! up once (and cached), as Polars does. `gs://` and `az://` start from the
//! provider's `from_env()` builder, then apply the explicit options. Any other scheme
//! (local, memory, http) goes straight to [`object_store::parse_url_opts`].
//!
//! Not covered, same as Polars: SSO / `aws login` / `credential_process` profiles, and
//! assume-role profiles. Export their credentials as `AWS_*` variables instead
//! (e.g. `aws configure export-credentials --format env`).

use crate::cloud_source::shared_runtime;
use object_store::aws::{resolve_bucket_region, AmazonS3, AmazonS3Builder, AmazonS3ConfigKey};
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::path::Path as ObjectPath;
use object_store::{ClientOptions, ObjectStore, ObjectStoreScheme};
use std::collections::HashMap;
use std::io::{Error as IoError, Result as IoResult};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use url::Url;

/// Resolve `url` plus `storage_options`-style `options` into a store and the object
/// path within it.
pub(crate) fn build_store(
    url: &Url,
    options: &[(String, String)],
) -> IoResult<(Box<dyn ObjectStore>, ObjectPath)> {
    let (scheme, raw_path) = ObjectStoreScheme::parse(url).map_err(IoError::other)?;
    let path = ObjectPath::parse(raw_path).map_err(IoError::other)?;
    let store: Box<dyn ObjectStore> = match scheme {
        ObjectStoreScheme::AmazonS3 => Box::new(build_s3(url, options)?),
        ObjectStoreScheme::GoogleCloudStorage => {
            let mut b = GoogleCloudStorageBuilder::from_env().with_url(url.to_string());
            for (k, v) in options {
                if let Ok(key) = k.to_ascii_lowercase().parse::<GoogleConfigKey>() {
                    b = b.with_config(key, v.clone());
                }
            }
            Box::new(b.build().map_err(IoError::other)?)
        }
        ObjectStoreScheme::MicrosoftAzure => {
            let mut b = MicrosoftAzureBuilder::from_env().with_url(url.to_string());
            for (k, v) in options {
                if let Ok(key) = k.to_ascii_lowercase().parse::<AzureConfigKey>() {
                    b = b.with_config(key, v.clone());
                }
            }
            Box::new(b.build().map_err(IoError::other)?)
        }
        _ => return object_store::parse_url_opts(url, options.to_vec()).map_err(IoError::other),
    };
    Ok((store, path))
}

fn build_s3(url: &Url, options: &[(String, String)]) -> IoResult<AmazonS3> {
    let mut b = s3_builder(url, options, std::env::vars(), home_dir().as_deref());

    let unset = |b: &AmazonS3Builder, k| b.get_config_value(&k).is_none();
    if unset(&b, AmazonS3ConfigKey::DefaultRegion) && unset(&b, AmazonS3ConfigKey::Region) {
        if !unset(&b, AmazonS3ConfigKey::Endpoint) {
            // A custom (S3-compatible) endpoint: there is no AWS region to look up.
            b = b.with_config(AmazonS3ConfigKey::Region, "us-east-1");
        } else if matches!(url.scheme(), "s3" | "s3a") {
            if let Some(region) = url.host_str().and_then(bucket_region) {
                b = b.with_config(AmazonS3ConfigKey::Region, region);
            }
        }
    }
    // What Polars does: skip hashing request payloads (the connection is TLS).
    b.with_unsigned_payload(true).build().map_err(IoError::other)
}

/// The S3 builder before any network lookups. `env` is injectable for testing.
fn s3_builder(
    url: &Url,
    options: &[(String, String)],
    env: impl IntoIterator<Item = (String, String)>,
    home: Option<&Path>,
) -> AmazonS3Builder {
    let mut b = AmazonS3Builder::new();
    // Same as `AmazonS3Builder::from_env`.
    for (k, v) in env {
        if k.starts_with("AWS_") {
            if let Ok(key) = k.to_ascii_lowercase().parse::<AmazonS3ConfigKey>() {
                b = b.with_config(key, v);
            }
        }
    }
    b = b.with_url(url.to_string());
    if let Some(home) = home {
        b = read_config(b, &home.join(".aws").join("config"), &[("region", AmazonS3ConfigKey::Region)]);
        b = read_config(
            b,
            &home.join(".aws").join("credentials"),
            &[
                ("aws_access_key_id", AmazonS3ConfigKey::AccessKeyId),
                ("aws_secret_access_key", AmazonS3ConfigKey::SecretAccessKey),
                ("aws_session_token", AmazonS3ConfigKey::Token),
            ],
        );
    }
    for (k, v) in options {
        if let Ok(key) = k.to_ascii_lowercase().parse::<AmazonS3ConfigKey>() {
            b = b.with_config(key, v.clone());
        }
    }
    b
}

/// The bucket's region, looked up once per process per bucket. `None` if the lookup
/// fails (e.g. offline); the store then keeps `object_store`'s default region.
fn bucket_region(bucket: &str) -> Option<String> {
    static CACHE: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    if let Some(region) = cache.lock().ok()?.get(bucket) {
        return Some(region.clone());
    }
    let region = shared_runtime()
        .block_on(resolve_bucket_region(bucket, &ClientOptions::new()))
        .ok()?;
    cache.lock().ok()?.insert(bucket.to_string(), region.clone());
    Some(region)
}

fn home_dir() -> Option<PathBuf> {
    ["HOME", "USERPROFILE"]
        .iter()
        .find_map(|k| std::env::var_os(k).filter(|v| !v.is_empty()))
        .map(PathBuf::from)
}

/// Port of polars-io's `read_config`: for each `(name, key)` not already set on the
/// builder, take the first `name = value` in the file and set `key` from it. Skipped
/// entirely if every key is already set or the file can't be read, and stops at the
/// first name that has no match (as Polars does).
fn read_config(
    mut b: AmazonS3Builder,
    path: &Path,
    entries: &[(&str, AmazonS3ConfigKey)],
) -> AmazonS3Builder {
    if entries.iter().all(|(_, key)| b.get_config_value(key).is_some()) {
        return b;
    }
    let Ok(content) = std::fs::read_to_string(path) else {
        return b;
    };
    for (name, key) in entries {
        if b.get_config_value(key).is_some() {
            continue;
        }
        let Some(value) = first_value(&content, name) else {
            break;
        };
        b = b.with_config(*key, value);
    }
    b
}

/// The value of the first `name <ws>* = <ws>* value` in `text`, up to the end of that
/// line — the match polars-io's regex `name\s*=\s*([^\r\n]*)` finds (case-sensitive,
/// not anchored to a line start, not section-aware). Surrounding whitespace is trimmed.
fn first_value<'a>(text: &'a str, name: &str) -> Option<&'a str> {
    let mut from = 0;
    while let Some(i) = text[from..].find(name) {
        let after = &text[from + i + name.len()..];
        let rest = after.trim_start();
        if let Some(rest) = rest.strip_prefix('=') {
            let rest = rest.trim_start();
            let line = rest.split(['\r', '\n']).next().unwrap_or("");
            return Some(line.trim());
        }
        from += i + name.len();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url() -> Url {
        Url::parse("s3://bucket/key.dta").unwrap()
    }

    fn pairs(v: &[(&str, &str)]) -> Vec<(String, String)> {
        v.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn get(b: &AmazonS3Builder, k: AmazonS3ConfigKey) -> Option<String> {
        b.get_config_value(&k)
    }

    /// A unique temp "home" with a `.aws` directory holding the given files; removed on drop.
    struct TempHome(PathBuf);
    impl TempHome {
        fn new(name: &str, credentials: Option<&str>, config: Option<&str>) -> Self {
            let home = std::env::temp_dir().join(format!("prs-cloud-config-{}-{name}", std::process::id()));
            std::fs::create_dir_all(home.join(".aws")).unwrap();
            if let Some(c) = credentials {
                std::fs::write(home.join(".aws").join("credentials"), c).unwrap();
            }
            if let Some(c) = config {
                std::fs::write(home.join(".aws").join("config"), c).unwrap();
            }
            Self(home)
        }
    }
    impl Drop for TempHome {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    const CREDENTIALS: &str = "\
# a comment
[default]
aws_access_key_id = DEFAULTKEY
aws_secret_access_key = DEFAULTSECRET

[work]
aws_access_key_id=WORKKEY
aws_secret_access_key = WORKSECRET
aws_session_token = WORKTOKEN
";
    const CONFIG: &str = "\
[default]
region = us-east-2

[profile work]
region = eu-west-1
";

    #[test]
    fn environment_is_applied_and_explicit_options_override_it() {
        let env = pairs(&[
            ("AWS_ACCESS_KEY_ID", "envkey"),
            ("AWS_SECRET_ACCESS_KEY", "envsecret"),
            ("AWS_REGION", "ap-south-1"),
            ("NOT_AWS_REGION", "ignored"),
        ]);
        let b = s3_builder(&url(), &pairs(&[("aws_access_key_id", "explicit")]), env, None);
        assert_eq!(get(&b, AmazonS3ConfigKey::AccessKeyId).as_deref(), Some("explicit"));
        assert_eq!(get(&b, AmazonS3ConfigKey::SecretAccessKey).as_deref(), Some("envsecret"));
        assert_eq!(get(&b, AmazonS3ConfigKey::Region).as_deref(), Some("ap-south-1"));
    }

    #[test]
    fn aws_files_fill_what_the_environment_did_not() {
        let home = TempHome::new("fill", Some(CREDENTIALS), Some(CONFIG));
        let env = pairs(&[("AWS_ACCESS_KEY_ID", "envkey")]);
        let b = s3_builder(&url(), &[], env, Some(&home.0));
        // env wins for the key it set; the first match in each file supplies the rest.
        assert_eq!(get(&b, AmazonS3ConfigKey::AccessKeyId).as_deref(), Some("envkey"));
        assert_eq!(get(&b, AmazonS3ConfigKey::SecretAccessKey).as_deref(), Some("DEFAULTSECRET"));
        assert_eq!(get(&b, AmazonS3ConfigKey::Region).as_deref(), Some("us-east-2"));
        // The first session token in the file is the one under [work] — Polars is not
        // profile-aware.
        assert_eq!(get(&b, AmazonS3ConfigKey::Token).as_deref(), Some("WORKTOKEN"));
    }

    #[test]
    fn aws_profile_is_ignored_like_polars() {
        let home = TempHome::new("profile", Some(CREDENTIALS), Some(CONFIG));
        let env = pairs(&[("AWS_PROFILE", "work")]);
        let b = s3_builder(&url(), &[], env, Some(&home.0));
        assert_eq!(get(&b, AmazonS3ConfigKey::AccessKeyId).as_deref(), Some("DEFAULTKEY"));
        assert_eq!(get(&b, AmazonS3ConfigKey::Region).as_deref(), Some("us-east-2"));
    }

    #[test]
    fn missing_files_are_not_errors() {
        let b = s3_builder(&url(), &[], Vec::new(), Some(Path::new("/nonexistent-home")));
        assert_eq!(get(&b, AmazonS3ConfigKey::AccessKeyId), None);
        assert_eq!(get(&b, AmazonS3ConfigKey::Region), None);
        let home = TempHome::new("nofiles", None, None);
        assert_eq!(get(&s3_builder(&url(), &[], Vec::new(), Some(&home.0)), AmazonS3ConfigKey::Region), None);
    }

    #[test]
    fn first_value_matches_the_polars_regex() {
        assert_eq!(first_value("region = us-east-1\n", "region"), Some("us-east-1"));
        assert_eq!(first_value("region=us-east-1", "region"), Some("us-east-1"));
        assert_eq!(first_value("a\n  region  =  r1  \r\nregion = r2", "region"), Some("r1"));
        // Not section-aware or line-anchored: a commented-out entry still matches.
        assert_eq!(first_value("# region = r0\nregion = r1", "region"), Some("r0"));
        assert_eq!(first_value("region_name = x\nregion = r1", "region"), Some("r1"));
        assert_eq!(first_value("no such key", "region"), None);
        // Case-sensitive, like the regex.
        assert_eq!(first_value("Region = r1", "region"), None);
    }
}

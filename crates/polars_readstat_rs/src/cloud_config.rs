//! Builds the [`ObjectStore`] for a cloud URL with the same credential and region
//! resolution Polars uses for `pl.scan_parquet("s3://…")`, so the two behave alike.
//!
//! For `s3://`, lowest to highest precedence:
//! 1. `AWS_*` environment variables (as `AmazonS3Builder::from_env`),
//! 2. the shared AWS files — `~/.aws/config` (region) and `~/.aws/credentials`
//!    (access key, secret, session token) for the `AWS_PROFILE` profile (default
//!    `default`), honouring `AWS_CONFIG_FILE` / `AWS_SHARED_CREDENTIALS_FILE`,
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
    let mut b = s3_builder(url, options, std::env::vars(), &AwsSharedFiles::from_env());

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
    files: &AwsSharedFiles,
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
    for (key, value) in files.settings() {
        if b.get_config_value(&key).is_none() {
            b = b.with_config(key, value);
        }
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

/// Locations and profile of the shared AWS config/credentials files.
struct AwsSharedFiles {
    credentials: Option<PathBuf>,
    config: Option<PathBuf>,
    profile: String,
}

impl AwsSharedFiles {
    fn from_env() -> Self {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        let home = var("HOME").or_else(|| var("USERPROFILE")).map(PathBuf::from);
        let in_home = |name: &str| home.as_ref().map(|h| h.join(".aws").join(name));
        Self {
            credentials: var("AWS_SHARED_CREDENTIALS_FILE")
                .map(PathBuf::from)
                .or_else(|| in_home("credentials")),
            config: var("AWS_CONFIG_FILE")
                .map(PathBuf::from)
                .or_else(|| in_home("config")),
            profile: var("AWS_PROFILE").unwrap_or_else(|| "default".to_string()),
        }
    }

    /// Settings found for the profile, as builder config entries.
    fn settings(&self) -> Vec<(AmazonS3ConfigKey, String)> {
        let mut out = Vec::new();
        let profile = self.profile.as_str();
        // The config file names non-default profiles `[profile NAME]`; the credentials
        // file uses `[NAME]`. Accept either form in both.
        let prefixed = format!("profile {profile}");
        let names = [profile, prefixed.as_str()];
        let entries = [
            (self.credentials.as_deref(), "aws_access_key_id", AmazonS3ConfigKey::AccessKeyId),
            (self.credentials.as_deref(), "aws_secret_access_key", AmazonS3ConfigKey::SecretAccessKey),
            (self.credentials.as_deref(), "aws_session_token", AmazonS3ConfigKey::Token),
            (self.config.as_deref(), "region", AmazonS3ConfigKey::Region),
        ];
        let mut sections: HashMap<&Path, Option<HashMap<String, String>>> = HashMap::new();
        for (file, ini_key, key) in entries {
            let Some(file) = file else { continue };
            let section = sections
                .entry(file)
                .or_insert_with(|| read_ini_section(file, &names));
            if let Some(value) = section.as_ref().and_then(|s| s.get(ini_key)) {
                out.push((key, value.clone()));
            }
        }
        out
    }
}

/// Key/value pairs of the first-listed matching sections of an INI file; `None` if the
/// file can't be read. Header whitespace is collapsed (`[profile  x]` == `[profile x]`),
/// keys are lowercased, and `#` / `;` lines are comments.
fn read_ini_section(path: &Path, names: &[&str]) -> Option<HashMap<String, String>> {
    let text = std::fs::read_to_string(path).ok()?;
    let mut out = HashMap::new();
    let mut inside = false;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some(header) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            let header = header.split_whitespace().collect::<Vec<_>>().join(" ");
            inside = names.contains(&header.as_str());
        } else if inside {
            if let Some((k, v)) = line.split_once('=') {
                out.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
            }
        }
    }
    Some(out)
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

    fn no_files() -> AwsSharedFiles {
        AwsSharedFiles { credentials: None, config: None, profile: "default".into() }
    }

    fn get(b: &AmazonS3Builder, k: AmazonS3ConfigKey) -> Option<String> {
        b.get_config_value(&k)
    }

    /// A unique temp file with `contents`, removed on drop.
    struct TempFile(PathBuf);
    impl TempFile {
        fn new(name: &str, contents: &str) -> Self {
            let p = std::env::temp_dir().join(format!("prs-cloud-config-{}-{name}", std::process::id()));
            std::fs::write(&p, contents).unwrap();
            Self(p)
        }
    }
    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
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
        let b = s3_builder(&url(), &pairs(&[("aws_access_key_id", "explicit")]), env, &no_files());
        assert_eq!(get(&b, AmazonS3ConfigKey::AccessKeyId).as_deref(), Some("explicit"));
        assert_eq!(get(&b, AmazonS3ConfigKey::SecretAccessKey).as_deref(), Some("envsecret"));
        assert_eq!(get(&b, AmazonS3ConfigKey::Region).as_deref(), Some("ap-south-1"));
    }

    #[test]
    fn shared_files_fill_what_the_environment_did_not() {
        let creds = TempFile::new("creds1", CREDENTIALS);
        let config = TempFile::new("config1", CONFIG);
        let files = AwsSharedFiles {
            credentials: Some(creds.0.clone()),
            config: Some(config.0.clone()),
            profile: "default".into(),
        };
        let env = pairs(&[("AWS_ACCESS_KEY_ID", "envkey")]);
        let b = s3_builder(&url(), &[], env, &files);
        // env wins for the key it set; the file supplies the rest.
        assert_eq!(get(&b, AmazonS3ConfigKey::AccessKeyId).as_deref(), Some("envkey"));
        assert_eq!(get(&b, AmazonS3ConfigKey::SecretAccessKey).as_deref(), Some("DEFAULTSECRET"));
        assert_eq!(get(&b, AmazonS3ConfigKey::Region).as_deref(), Some("us-east-2"));
        assert_eq!(get(&b, AmazonS3ConfigKey::Token), None);
    }

    #[test]
    fn named_profile_is_selected_from_both_files() {
        let creds = TempFile::new("creds2", CREDENTIALS);
        let config = TempFile::new("config2", CONFIG);
        let files = AwsSharedFiles {
            credentials: Some(creds.0.clone()),
            config: Some(config.0.clone()),
            profile: "work".into(),
        };
        let b = s3_builder(&url(), &[], Vec::new(), &files);
        assert_eq!(get(&b, AmazonS3ConfigKey::AccessKeyId).as_deref(), Some("WORKKEY"));
        assert_eq!(get(&b, AmazonS3ConfigKey::SecretAccessKey).as_deref(), Some("WORKSECRET"));
        assert_eq!(get(&b, AmazonS3ConfigKey::Token).as_deref(), Some("WORKTOKEN"));
        assert_eq!(get(&b, AmazonS3ConfigKey::Region).as_deref(), Some("eu-west-1"));
    }

    #[test]
    fn missing_files_and_unknown_profiles_are_not_errors() {
        let files = AwsSharedFiles {
            credentials: Some(PathBuf::from("/nonexistent/credentials")),
            config: None,
            profile: "nope".into(),
        };
        let b = s3_builder(&url(), &[], Vec::new(), &files);
        assert_eq!(get(&b, AmazonS3ConfigKey::AccessKeyId), None);

        let creds = TempFile::new("creds3", CREDENTIALS);
        let files = AwsSharedFiles { credentials: Some(creds.0.clone()), config: None, profile: "nope".into() };
        assert_eq!(get(&s3_builder(&url(), &[], Vec::new(), &files), AmazonS3ConfigKey::AccessKeyId), None);
    }

    #[test]
    fn ini_parser_handles_comments_spacing_and_header_whitespace() {
        let f = TempFile::new("ini", "; c\n[  profile   x ]\n  Region =  r1 \n# c\n[other]\nregion=r2\n");
        let s = read_ini_section(&f.0, &["profile x"]).unwrap();
        assert_eq!(s.get("region").map(String::as_str), Some("r1"));
        assert_eq!(s.len(), 1);
    }
}

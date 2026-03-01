use std::env;
use std::process::Command;

fn should_run(name: &str) -> bool {
    let setting = match env::var("READSTAT_RUN_PY_COMPARE") {
        Ok(v) => v.trim().to_lowercase(),
        Err(_) => return false,
    };
    if setting == "1" || setting == "true" || setting == "all" {
        return true;
    }
    setting
        .split(',')
        .map(|v| v.trim())
        .any(|v| v == name)
}

fn run_compare(name: &str, script: &str) {
    if !should_run(name) {
        eprintln!(
            "Skipping {name} (set READSTAT_RUN_PY_COMPARE=all or include '{name}')"
        );
        return;
    }

    let status = Command::new("uv")
        .arg("run")
        .arg(script)
        .env("UV_CACHE_DIR", "/tmp/uv-cache")
        .status()
        .expect("failed to run uv");

    assert!(
        status.success(),
        "{name} compare failed with status {status}"
    );
}

#[test]
fn compare_sas_to_python() {
    run_compare("sas", "tests/sas/compare_to_python.py");
}

#[test]
fn compare_spss_to_python() {
    run_compare("spss", "tests/spss/compare_to_python.py");
}

#[test]
fn compare_stata_to_python() {
    run_compare("stata", "tests/stata/compare_to_python.py");
}

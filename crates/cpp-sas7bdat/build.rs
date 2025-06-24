// build.rs
use std::env;
use std::path::PathBuf;
use std::fs;
use std::process::Command;

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());

    build_cpp_project(&manifest_dir);
    generate_bindings(&out_dir);
    link_prebuilt_library(&manifest_dir);
 
    println!("cargo:rerun-if-changed=vendor/include/cppsas7bdat/sink/arrow.hpp");
    println!("cargo:rerun-if-changed=vendor/src/arrow_ffi.cpp");
    println!("cargo:rerun-if-changed=vendor/src/arrow_ffi.h");
    println!("cargo:rerun-if-changed=build.rs");
}

fn build_cpp_project(manifest_dir: &PathBuf) {
    let vendor_dir = manifest_dir.join("vendor");
    
    println!("cargo:warning=Building C++ project in vendor directory");
    println!("cargo:warning=Current working directory: {}", env::current_dir().unwrap().display());
    println!("cargo:warning=Manifest directory: {}", manifest_dir.display());
    

    // Check if uv is available
    let uv_check = Command::new("uv")
        .arg("--version")
        .output();
    
    if uv_check.is_err() {
        panic!("uv is not available in PATH. Please install uv or ensure it's in your PATH.");
    }
    
    println!("cargo:warning=Syncing Python dependencies with uv");
    
    // First, sync dependencies to ensure everything is up-to-date
    let sync_output = Command::new("uv")
        .arg("sync")
        .output()
        .expect("Failed to execute uv sync");

    if !sync_output.status.success() {
        let stderr = String::from_utf8_lossy(&sync_output.stderr);
        let stdout = String::from_utf8_lossy(&sync_output.stdout);
        panic!(
            "uv sync failed!\nSTDOUT:\n{}\nSTDERR:\n{}", 
            stdout, stderr
        );
    }
    
    println!("cargo:warning=Using uv to run make build with project environment");
    
    // Use uv run - it will auto-detect the project
    let output = Command::new("make")
        .arg("build")
        .current_dir(&vendor_dir)  // Run make in the vendor directory
        .output()
        .expect("Failed to execute make build");

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        panic!(
            "C++ build failed!\nSTDOUT:\n{}\nSTDERR:\n{}", 
            stdout, stderr
        );
    }
    
    println!("cargo:warning=C++ build completed successfully");
}


fn generate_bindings(out_dir: &PathBuf) {
    let bindings = bindgen::Builder::default()
        .header("vendor/src/arrow_ffi.h")
        .clang_arg("-Ivendor/src")
        .clang_arg("-Ivendor/include")
        .clang_arg("-DCPPSAS7BDAT_HAVE_ARROW")
        .clang_arg("-std=c++17")
        .clang_arg("-x")
        .clang_arg("c++")
        // Arrow FFI interface - the NEW API
        .allowlist_function("sas_arrow_.*")
        .allowlist_type("SasArrowReader")
        .allowlist_type("SasArrowErrorCode")
        .allowlist_type("SasArrowReaderInfo")
        .allowlist_type("SasArrowColumnInfo")
        .allowlist_type("ArrowArray")
        .allowlist_type("ArrowSchema")
        .allowlist_var("SAS_ARROW_.*")
        // Block the old chunked reader API to force migration
        .blocklist_function("chunked_reader_.*")
        .blocklist_function("chunk_iterator_.*")
        .generate()
        .expect("Unable to generate bindings");

    bindings
        .write_to_file(out_dir.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}

fn link_prebuilt_library(manifest_dir: &PathBuf) {
    // Point to your pre-built library
    let lib_dir = manifest_dir.join("vendor/build/Release/src");
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    
    // Dependencies directory from Conan (where Makefile puts them)
    let deps_dir = manifest_dir.join("vendor/build/dependencies/direct_deploy");
    
    // Add search paths for all dependency libraries
    let arrow_lib_dir = deps_dir.join("arrow/lib");
    let boost_lib_dir = deps_dir.join("boost/lib");
    let spdlog_lib_dir = deps_dir.join("spdlog/lib");
    let fmt_lib_dir = deps_dir.join("fmt/lib");
    
    // Debug: Print what directories exist
    println!("cargo:warning=Checking dependency directories:");
    println!("cargo:warning=Arrow lib dir exists: {}", arrow_lib_dir.exists());
    println!("cargo:warning=Boost lib dir exists: {}", boost_lib_dir.exists());
    println!("cargo:warning=Spdlog lib dir exists: {}", spdlog_lib_dir.exists());
    println!("cargo:warning=Fmt lib dir exists: {}", fmt_lib_dir.exists());
    
    if arrow_lib_dir.exists() {
        println!("cargo:rustc-link-search=native={}", arrow_lib_dir.display());
        if let Ok(entries) = fs::read_dir(&arrow_lib_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    println!("cargo:warning=Found in arrow/lib: {}", name);
                }
            }
        }
    }
    if boost_lib_dir.exists() {
        println!("cargo:rustc-link-search=native={}", boost_lib_dir.display());
        if let Ok(entries) = fs::read_dir(&boost_lib_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    println!("cargo:warning=Found in boost/lib: {}", name);
                }
            }
        }
    }
    if spdlog_lib_dir.exists() {
        println!("cargo:rustc-link-search=native={}", spdlog_lib_dir.display());
        if let Ok(entries) = fs::read_dir(&spdlog_lib_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    println!("cargo:warning=Found in spdlog/lib: {}", name);
                }
            }
        }
    }
    if fmt_lib_dir.exists() {
        println!("cargo:rustc-link-search=native={}", fmt_lib_dir.display());
        if let Ok(entries) = fs::read_dir(&fmt_lib_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    println!("cargo:warning=Found in fmt/lib: {}", name);
                }
            }
        }
    }

    // Link dependencies FIRST (before main library)
    // This is important for static linking order
    
    // Link fmt first (spdlog depends on it)
    if fmt_lib_dir.exists() {
        println!("cargo:rustc-link-lib=static=fmt");
    }
    
    // Link spdlog (depends on fmt)
    if spdlog_lib_dir.exists() {
        println!("cargo:rustc-link-lib=static=spdlog");
    }
    
    // Link Boost libraries
    if boost_lib_dir.exists() {
        link_boost_libraries(&boost_lib_dir);
    }
    
    // Link Arrow
    if arrow_lib_dir.exists() {
        println!("cargo:rustc-link-lib=static=arrow");
    }

    // Link the main static library LAST (it depends on the others)
    println!("cargo:rustc-link-lib=static=cppsas7bdat");

    // Link only essential system libraries
    if cfg!(target_os = "linux") {
        println!("cargo:rustc-link-lib=pthread");
        println!("cargo:rustc-link-lib=dl");
        println!("cargo:rustc-link-lib=stdc++");
        println!("cargo:rustc-link-lib=m");
    } else if cfg!(target_os = "macos") {
        println!("cargo:rustc-link-lib=c++");
        println!("cargo:rustc-link-lib=System");
    }
}

fn link_boost_libraries(boost_lib_dir: &PathBuf) {
    // Read the boost lib directory and link all boost libraries found
    if let Ok(entries) = fs::read_dir(boost_lib_dir) {
        for entry in entries.flatten() {
            if let Some(file_name) = entry.file_name().to_str() {
                // Look for libboost_*.a files
                if file_name.starts_with("libboost_") && file_name.ends_with(".a") {
                    // Extract library name: libboost_system.a -> boost_system
                    if let Some(lib_name) = file_name.strip_prefix("lib").and_then(|s| s.strip_suffix(".a")) {
                        println!("cargo:rustc-link-lib=static={}", lib_name);
                    }
                }
            }
        }
    }
}
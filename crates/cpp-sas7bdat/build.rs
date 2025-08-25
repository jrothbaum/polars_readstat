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
    
    
   
    println!("cargo:warning=Run make build with project environment");
    
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
        .generate()
        .expect("Unable to generate bindings");

    bindings
        .write_to_file(out_dir.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}

// New helper function to recursively search for library files
fn find_library_files(search_dir: &PathBuf, search_patterns: &[&str]) -> Vec<PathBuf> {
    let mut found_files = Vec::new();
    
    fn search_recursive(dir: &PathBuf, patterns: &[&str], found: &mut Vec<PathBuf>) {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    search_recursive(&path, patterns, found);
                } else if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                    for pattern in patterns {
                        if file_name.contains(pattern) {
                            found.push(path.clone());
                        }
                    }
                }
            }
        }
    }
    
    search_recursive(search_dir, search_patterns, &mut found_files);
    found_files
}

fn link_prebuilt_library(manifest_dir: &PathBuf) {
    // Different build systems create different directory structures
    let possible_lib_dirs = if cfg!(target_os = "windows") {
        // Windows (Visual Studio/MSBuild) structure
        vec![
            manifest_dir.join("vendor/build/src/Release"),
            manifest_dir.join("vendor/build/Release/src"),
            manifest_dir.join("vendor/build/Release"),
        ]
    } else {
        // Unix (Make) structure
        vec![
            manifest_dir.join("vendor/build/src"),
            manifest_dir.join("vendor/build/Release/src"),
            manifest_dir.join("vendor/build"),
        ]
    };
    
    // Find the directory that actually contains our library
    let mut lib_dir = None;
    for dir in &possible_lib_dirs {
        println!("cargo:warning=Checking potential lib dir: {}", dir.display());
        let test_lib_path = if cfg!(target_os = "windows") {
            dir.join("cppsas7bdat.lib")
        } else {
            dir.join("libcppsas7bdat.a")
        };
        
        if test_lib_path.exists() {
            println!("cargo:warning=Found main library at: {}", test_lib_path.display());
            lib_dir = Some(dir.clone());
            break;
        }
    }
    
    let lib_dir = lib_dir.unwrap_or_else(|| {
        println!("cargo:warning=No library found in any expected location, using default");
        if cfg!(target_os = "windows") {
            manifest_dir.join("vendor/build/src/Release")
        } else {
            manifest_dir.join("vendor/build/src")
        }
    });
    
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
    
    // On Windows, find and add the path for the shared iconv library
    if cfg!(target_os = "windows") {
        if let Ok(iconv_dir) = env::var("DEP_ICONV_ROOT") {
            let iconv_lib_path = PathBuf::from(iconv_dir).join("lib");
            if iconv_lib_path.exists() {
                println!("cargo:rustc-link-search=native={}", iconv_lib_path.display());
                println!("cargo:warning=✅ Found and added iconv-sys library path: {}", iconv_lib_path.display());
            } else {
                println!("cargo:warning=❌ iconv-sys lib path does not exist: {}", iconv_lib_path.display());
            }
        } else {
            println!("cargo:warning=❌ DEP_ICONV_ROOT env var not set. Ensure iconv-sys is a build-dependency. Link will likely fail.");
        }
    }

    // Debug: Check if main library exists in the selected directory
    let main_lib_path = if cfg!(target_os = "windows") {
        lib_dir.join("cppsas7bdat.lib")
    } else {
        lib_dir.join("libcppsas7bdat.a")
    };
    println!("cargo:warning=Final main library path: {}", main_lib_path.display());
    println!("cargo:warning=Final main library exists: {}", main_lib_path.exists());
    
    // NEW: Comprehensive search for the main library file
    println!("cargo:warning=================================");
    println!("cargo:warning=COMPREHENSIVE LIBRARY SEARCH");
    println!("cargo:warning=================================");
    
    // Search patterns for the main library
    let main_lib_patterns = ["cppsas7bdat", "sas7bdat"];
    
    // Search in multiple possible locations
    let search_locations = [
        manifest_dir.join("vendor/build"),
        manifest_dir.join("vendor/build/Release"),
        manifest_dir.join("vendor/build/Debug"),
        manifest_dir.join("vendor/build/src"),
        manifest_dir.join("vendor/build/Release/src"),
        manifest_dir.join("vendor/build/Debug/src"),
        manifest_dir.join("vendor/src"),
        manifest_dir.join("vendor/lib"),
        manifest_dir.join("vendor"),
    ];
    
    println!("cargo:warning=Searching for main library files containing: {:?}", main_lib_patterns);
    
    for location in &search_locations {
        if location.exists() {
            println!("cargo:warning=Searching in: {}", location.display());
            let found_files = find_library_files(location, &main_lib_patterns);
            
            if !found_files.is_empty() {
                println!("cargo:warning=Found {} potential main library files in {}:", found_files.len(), location.display());
                for file in &found_files {
                    println!("cargo:warning=  MAIN LIBRARY CANDIDATE -> {}", file.display());
                }
            } else {
                println!("cargo:warning=No main library files found in {}", location.display());
            }
        } else {
            println!("cargo:warning=Directory doesn't exist: {}", location.display());
        }
    }
    
    // NEW: Also search for ANY .lib files in the build directory
    println!("cargo:warning=--------------------------------");
    println!("cargo:warning=SEARCHING FOR ALL .lib FILES");
    println!("cargo:warning=--------------------------------");
    
    let build_dir = manifest_dir.join("vendor/build");
    if build_dir.exists() {
        let all_lib_files = find_library_files(&build_dir, &[".lib"]);
        println!("cargo:warning=Found {} .lib files in build directory (showing first 20):", all_lib_files.len());
        for (i, file) in all_lib_files.iter().enumerate() {
            if i >= 20 { 
                println!("cargo:warning=  ... and {} more .lib files", all_lib_files.len() - 20);
                break; 
            }
            println!("cargo:warning=  .lib file: {}", file.display());
        }
    }
    
    // NEW: Also search for ANY .a files (in case it's building static libraries with different extension)
    let all_a_files = find_library_files(&build_dir, &[".a"]);
    if !all_a_files.is_empty() {
        println!("cargo:warning=Found {} .a files in build directory:", all_a_files.len());
        for file in &all_a_files {
            println!("cargo:warning=  .a file: {}", file.display());
        }
    }
    
    // NEW: Let's also check the make output more carefully
    println!("cargo:warning=--------------------------------");
    println!("cargo:warning=CHECKING MAKE OUTPUT DIRECTORY");
    println!("cargo:warning=--------------------------------");
    
    // Check various build output directories that make might use
    let possible_output_dirs = [
        manifest_dir.join("vendor/build"),
        manifest_dir.join("vendor/build/src"),
        manifest_dir.join("vendor/build/Release"),
        manifest_dir.join("vendor/build/Release/src"),
        manifest_dir.join("vendor/build/Debug"),
        manifest_dir.join("vendor/build/Debug/src"),
        manifest_dir.join("vendor/out"),
        manifest_dir.join("vendor/lib"),
    ];
    
    for dir in &possible_output_dirs {
        if dir.exists() {
            println!("cargo:warning=Checking directory: {}", dir.display());
            if let Ok(entries) = fs::read_dir(dir) {
                let mut file_count = 0;
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_file() {
                        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                            if name.ends_with(".lib") || name.ends_with(".a") || name.contains("cppsas7bdat") || name.contains("sas7bdat") {
                                println!("cargo:warning=  IMPORTANT FILE: {}", path.display());
                            }
                            file_count += 1;
                        }
                    }
                }
                println!("cargo:warning=  Total files in directory: {}", file_count);
            }
        }
    }
    
    // List all files in the main lib directory for debugging
    if lib_dir.exists() {
        println!("cargo:warning=Contents of main lib directory:");
        if let Ok(entries) = fs::read_dir(&lib_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    println!("cargo:warning=Found in main lib dir: {}", name);
                }
            }
        }
    }
    
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
        link_arrow_library(&arrow_lib_dir);
    }

    // Link the main static library LAST (it depends on the others)
    // Try different possible library names for Windows
    let possible_main_libs = [
        "cppsas7bdat_bundled",  // Try bundled version first
        "cppsas7bdat",
        "libcppsas7bdat", 
        "cppsas7bdat_static"
    ];

    if cfg!(target_os = "windows") {
        println!("cargo:rustc-link-lib=iconv");
    }
    
    let mut found_main_lib = false;
    for lib_name in &possible_main_libs {
        let lib_file = if cfg!(target_os = "windows") {
            format!("{}.lib", lib_name)
        } else {
            format!("lib{}.a", lib_name)
        };
        
        let lib_path = lib_dir.join(&lib_file);
        if lib_path.exists() {
            println!("cargo:rustc-link-lib=static={}", lib_name);
            println!("cargo:warning=Successfully found and linked main library: {} at {}", lib_name, lib_path.display());
            found_main_lib = true;
            break;
        }
    }
    
    if !found_main_lib {
        println!("cargo:warning=WARNING: Main library not found! Trying bundled version as fallback.");
        println!("cargo:rustc-link-lib=static=cppsas7bdat_bundled");
    }

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

fn link_arrow_library(arrow_lib_dir: &PathBuf) {
    // Check what Arrow library files actually exist and link accordingly
    let mut found_arrow = false;
    
    if let Ok(entries) = fs::read_dir(arrow_lib_dir) {
        for entry in entries.flatten() {
            if let Some(file_name) = entry.file_name().to_str() {
                // Windows: look for arrow_static.lib
                if cfg!(target_os = "windows") && file_name == "arrow_static.lib" {
                    println!("cargo:rustc-link-lib=static=arrow_static");
                    println!("cargo:warning=Linked Windows Arrow library: arrow_static");
                    found_arrow = true;
                    break;
                }
                // Unix: look for libarrow.a or libarrow_static.a
                else if !cfg!(target_os = "windows") {
                    if file_name == "libarrow_static.a" {
                        println!("cargo:rustc-link-lib=static=arrow_static");
                        println!("cargo:warning=Linked Unix Arrow library: arrow_static");
                        found_arrow = true;
                        break;
                    } else if file_name == "libarrow.a" {
                        println!("cargo:rustc-link-lib=static=arrow");
                        println!("cargo:warning=Linked Unix Arrow library: arrow");
                        found_arrow = true;
                        break;
                    }
                }
            }
        }
    }
    
    if !found_arrow {
        println!("cargo:warning=No Arrow library found! Falling back to default linking");
        // Fallback - try the default name
        if cfg!(target_os = "windows") {
            println!("cargo:rustc-link-lib=static=arrow_static");
        } else {
            println!("cargo:rustc-link-lib=static=arrow");
        }
    }
}
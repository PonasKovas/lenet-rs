use std::env;
use std::path::PathBuf;

fn main() {
    // Locate the built Lenet C distribution.
    // LENET_LIB_DIR overrides; otherwise default to the sibling lenet checkout.
    let lib_dir: PathBuf = env::var_os("LENET_LIB_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
            manifest
                .parent()
                .expect("manifest dir has a parent")
                .join("..")
                .join("lenet")
                .join("csrc")
                .join("build")
        });

    let lib = lib_dir.join("liblenet.a");
    if !lib.exists() {
        panic!(
            "liblenet.a not found at {}. Build it with `make -C <lenet>/csrc`, \
             or set LENET_LIB_DIR to the directory containing it.",
            lib.display()
        );
    }

    // Recompile when the archive changes.
    println!("cargo:rerun-if-changed={}", lib.display());
    println!("cargo:rerun-if-env-changed=LENET_LIB_DIR");

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=static=lenet");
    // Runtime deps of the Lean runtime, per <lenet>/csrc/Makefile's check target.
    println!("cargo:rustc-link-lib=pthread");
    println!("cargo:rustc-link-lib=dl");
    println!("cargo:rustc-link-lib=m");
}

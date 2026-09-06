use cmake::Config;

fn main() {
    // Build ENet (upstream, pinned in the sibling checkout) as a static lib.
    // ENET_DIR overrides the checkout location.
    let enet_dir = std::env::var_os("ENET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("../../enet"));
    let dst = Config::new(enet_dir).build();
    println!(
        "cargo:rustc-link-search=native={}",
        dst.join("lib").display()
    );
    println!(
        "cargo:rustc-link-search=native={}",
        dst.join("build").display()
    );
    println!("cargo:rustc-link-lib=static=enet");
    println!("cargo:rerun-if-env-changed=ENET_DIR");
    println!("cargo:rerun-if-changed=helper.c");
    let mut build = cc::Build::new();
    build.file("helper.c");
    for inc in [dst.join("include"), dst.join("build").join("include")] {
        if inc.join("enet").join("enet.h").exists() {
            build.include(&inc);
        }
    }
    build.warnings(true).compile("enet_helper");
}

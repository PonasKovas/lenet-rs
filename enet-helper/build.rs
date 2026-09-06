//! Build script for the ENet interop test oracle.
//!
//! By default it fetches upstream ENet (pinned revision) into this build's
//! directory, builds it with cmake, and compiles `helper.c` against it.
//! Set `ENET_DIR` to an existing ENet checkout to build from that instead.

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

const ENET_REPO: &str = "https://github.com/lsalzman/enet";
const ENET_REV: &str = "5a9c537fd464b3c6d3c55e1d3bd47588faf71b42";

fn fail(msg: &str) -> ! {
    eprintln!("\nenet-helper build script: {msg}");
    std::process::exit(1);
}

fn git(args: &[&str], repo_arg: &Path) {
    let mut cmd = Command::new("git");
    cmd.arg("-C").arg(repo_arg).args(args);
    let status = cmd
        .status()
        .unwrap_or_else(|e| fail(&format!("failed to spawn git: {e}")));
    if !status.success() {
        fail(&format!("git {} failed with {status}", args.join(" ")));
    }
}

/// ENet checkout: `ENET_DIR` (developer checkout) or a pinned clone in
/// this build's OUT_DIR.
fn resolve_sources(out_dir: &Path) -> PathBuf {
    if let Some(dir) = env::var_os("ENET_DIR") {
        return PathBuf::from(dir);
    }

    let dir = out_dir.join("enet");
    if !dir.join(".git").exists() {
        println!(
            "cargo:warning=enet-helper: fetching ENet (pinned {})",
            &ENET_REV[..8]
        );
        let status = Command::new("git")
            .args(["clone", "--quiet", ENET_REPO])
            .arg(&dir)
            .status()
            .unwrap_or_else(|e| fail(&format!("failed to spawn git: {e} (is git installed?)")));
        if !status.success() {
            fail("git clone of https://github.com/lsalzman/enet failed");
        }
    }
    // Move to the pinned revision (fetching first if we don't have it),
    // then repair any local damage (interrupted clone, partial writes).
    let at_rev = Command::new("git")
        .arg("-C")
        .arg(&dir)
        .args(["checkout", "--quiet"])
        .arg(ENET_REV)
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if !at_rev {
        git(&["fetch", "origin"], &dir);
        git(&["checkout", "--quiet", ENET_REV], &dir);
    }
    git(&["reset", "--hard", "--quiet", ENET_REV], &dir);

    // Sentinel check: a damaged checkout is wiped and re-cloned.
    let healthy = |d: &Path| {
        std::fs::metadata(d.join("include").join("enet").join("enet.h"))
            .map(|m| m.len() > 0)
            .unwrap_or(false)
    };
    if !healthy(&dir) {
        println!("cargo:warning=enet-helper: ENet checkout damaged; re-fetching");
        std::fs::remove_dir_all(&dir)
            .unwrap_or_else(|e| fail(&format!("cannot remove damaged clone: {e}")));
        let status = Command::new("git")
            .args(["clone", "--quiet", ENET_REPO])
            .arg(&dir)
            .status()
            .unwrap_or_else(|e| fail(&format!("failed to spawn git: {e}")));
        if !status.success() {
            fail("git clone of https://github.com/lsalzman/enet failed");
        }
        git(&["checkout", "--quiet", ENET_REV], &dir);
        git(&["reset", "--hard", "--quiet", ENET_REV], &dir);
        if !healthy(&dir) {
            fail("re-cloned ENet checkout still looks damaged");
        }
    }
    dir
}

fn main() {
    println!("cargo:rerun-if-env-changed=ENET_DIR");
    println!("cargo:rerun-if-changed=helper.c");

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR set by cargo"));
    let enet_dir = resolve_sources(&out_dir);

    let dst = cmake::Config::new(&enet_dir).build();
    println!(
        "cargo:rustc-link-search=native={}",
        dst.join("lib").display()
    );
    println!(
        "cargo:rustc-link-search=native={}",
        dst.join("build").display()
    );
    println!("cargo:rustc-link-lib=static=enet");

    let mut build = cc::Build::new();
    build.file("helper.c");
    for inc in [dst.join("include"), dst.join("build").join("include")] {
        if inc.join("enet").join("enet.h").exists() {
            build.include(&inc);
        }
    }
    build.warnings(true).compile("enet_helper");
}

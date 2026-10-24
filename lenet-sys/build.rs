//! Build script: builds `liblenet.a` from a pinned Lenet revision.
//!
//! By default it clones the Lenet sources (a separate repository) at
//! `LENET_REV` into this crate's build directory and builds `liblenet.a`
//! with the Lenet repo's own Makefile. The build is reproducible: the same
//! lenet-rs commit always builds the same Lenet code.
//!
//! Requirements: git, cc/make, a network for the first clone, and the Lean
//! toolchain manager (elan, https://github.com/leanprover/elan). Nothing is
//! installed for you: elan itself fetches the Lean version Lenet pins in
//! its `lean-toolchain` file the first time it is used.
//!
//! Local development against a Lenet checkout: set `LENET_DIR` to the repo
//! root (no fetching). The build is incremental — editing Lenet sources
//! triggers the (fast) rebuild of just what changed.

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

const LENET_REPO: &str = "https://github.com/PonasKovas/lenet";
/// The Lenet commit this crate's FFI declarations match (`lenet.h`).
/// Bump it together with `src/lib.rs`.
const LENET_REV: &str = "d532417e3870dc55c74490d81ea35adb435bb53e";

fn fail(msg: &str) -> ! {
    eprintln!("\nlenet-sys build script: {msg}");
    std::process::exit(1);
}

fn run(desc: &str, mut cmd: Command) {
    let status = cmd.status().unwrap_or_else(|e| {
        fail(&format!(
            "failed to spawn `{desc}`: {e} (is it installed and on PATH?)"
        ))
    });
    if !status.success() {
        fail(&format!("`{desc}` failed with {status}"));
    }
}

/// `git -C <repo> <args>`, failing the build on error.
fn git(args: &[&str], repo: &Path) {
    let mut cmd = Command::new("git");
    cmd.arg("-C").arg(repo).args(args);
    run(&format!("git {} <lenet>", args.join(" ")), cmd);
}

/// `git -C <repo> <args>`: true on success. For probes that may fail.
fn git_ok(args: &[&str], repo: &Path) -> bool {
    Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn clone(dir: &Path) {
    println!(
        "cargo:warning=lenet-sys: cloning Lenet (pinned {})",
        &LENET_REV[..8]
    );
    let mut cmd = Command::new("git");
    cmd.args(["clone", "--quiet", LENET_REPO]).arg(dir);
    run(&format!("git clone {LENET_REPO}"), cmd);
}

/// The elan bin directory, where a standard elan install puts `lake`.
fn elan_bin() -> Option<PathBuf> {
    env::var_os("HOME").map(|home| PathBuf::from(home).join(".elan").join("bin"))
}

/// True if `name` is an existing file in the elan bin dir or on PATH.
fn have_tool(name: &str) -> bool {
    let path = env::var_os("PATH").unwrap_or_default();
    elan_bin()
        .into_iter()
        .chain(env::split_paths(&path))
        .any(|d| d.join(name).is_file())
}

/// PATH with the elan bin dir prepended, for spawned build commands.
fn path_with_elan() -> std::ffi::OsString {
    let path = env::var_os("PATH").unwrap_or_default();
    let dirs = elan_bin().into_iter().chain(env::split_paths(&path));
    env::join_paths(dirs).expect("PATH join")
}

/// Where the Lenet sources live: `LENET_DIR` (developer checkout) or a
/// clone at `LENET_REV` inside this build's OUT_DIR.
fn resolve_sources(out_dir: &Path) -> PathBuf {
    println!("cargo:rerun-if-env-changed=LENET_DIR");
    if let Some(dir) = env::var_os("LENET_DIR") {
        return PathBuf::from(dir);
    }

    let dir = out_dir.join("lenet");
    if !dir.join(".git").exists() {
        clone(&dir);
    }
    // Fetch only when the pinned commit is missing (e.g. after a bump), so
    // builds after the first one work offline.
    let have_rev = git_ok(
        &["cat-file", "-e", &format!("{LENET_REV}^{{commit}}")],
        &dir,
    );
    if !have_rev {
        git(&["fetch", "--quiet", "origin"], &dir);
    }
    // Also repairs local damage (an interrupted checkout, edited files).
    git(&["reset", "--hard", "--quiet", LENET_REV], &dir);

    // Sentinel check: a damaged clone (e.g. zero-length files after a
    // crash) is wiped and cloned again rather than trusted.
    let healthy = |d: &Path| {
        [
            "lean-toolchain",
            "csrc/lenet_capi.c",
            "csrc/include/lenet.h",
        ]
        .iter()
        .all(|f| std::fs::metadata(d.join(f)).is_ok_and(|m| m.len() > 0))
    };
    if !healthy(&dir) {
        println!("cargo:warning=lenet-sys: Lenet clone damaged; cloning again");
        std::fs::remove_dir_all(&dir)
            .unwrap_or_else(|e| fail(&format!("cannot remove damaged clone: {e}")));
        clone(&dir);
        git(&["reset", "--hard", "--quiet", LENET_REV], &dir);
        if !healthy(&dir) {
            fail("the fresh Lenet clone still looks damaged");
        }
    }
    dir
}

/// Emit rerun-if-changed for every Lenet source file (skipping build
/// outputs), so editing a `LENET_DIR` checkout rebuilds exactly as needed.
fn track_sources(root: &Path) {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        println!("cargo:rerun-if-changed={}", dir.display());
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            match entry.file_type() {
                Ok(t) if t.is_dir() => {
                    if matches!(name, ".git" | ".lake") || path.ends_with("csrc/build") {
                        continue; // build outputs: not sources
                    }
                    stack.push(path);
                }
                Ok(_) => println!("cargo:rerun-if-changed={}", path.display()),
                Err(_) => {}
            }
        }
    }
}

/// Builds `csrc/build/liblenet.a` with the Lenet repo's own Makefile
/// (incremental: a no-op when everything is current).
fn ensure_built(src: &Path) {
    let lib = src.join("csrc").join("build").join("liblenet.a");

    if !have_tool("lake") {
        if lib.exists() {
            // A prebuilt checkout without a Lean install: link it as-is.
            return;
        }
        fail(
            "the Lean toolchain is needed to build Lenet, but `lake` was not \
             found on PATH or in ~/.elan/bin. Install elan \
             (https://github.com/leanprover/elan#installation); it fetches \
             the Lean version Lenet pins on first use. Alternatively, set \
             LENET_DIR to a Lenet checkout with a prebuilt \
             csrc/build/liblenet.a.",
        );
    }

    if !lib.exists() {
        println!(
            "cargo:warning=lenet-sys: building Lenet (Lean compile; first build takes a few minutes)"
        );
    }

    let mut cmd = Command::new("make");
    cmd.arg("-C").arg(src.join("csrc"));
    cmd.env("PATH", path_with_elan());
    run("make -C <lenet>/csrc", cmd);

    if !lib.exists() {
        fail(&format!("make succeeded but {} is missing", lib.display()));
    }
}

fn main() {
    // The target, not the machine running this script.
    if env::var("CARGO_CFG_TARGET_FAMILY").as_deref() != Ok("unix") {
        fail(
            "only Unix targets are supported: the Lean static runtime is \
             built without -fPIC TLS (shared libraries are impossible) and \
             the build tooling assumes a POSIX shell. Use Linux or macOS.",
        );
    }

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR set by cargo"));
    let src = resolve_sources(&out_dir);
    if !src.join("csrc").join("lenet_capi.c").is_file() {
        fail(&format!(
            "{} does not look like a Lenet checkout (missing csrc/lenet_capi.c). \
             Point LENET_DIR at the repository root.",
            src.display()
        ));
    }

    if env::var_os("LENET_DIR").is_some() {
        track_sources(&src);
    } else {
        // The pinned clone only changes when LENET_REV (in this file) does.
        println!("cargo:rerun-if-changed=build.rs");
    }
    ensure_built(&src);

    let build_dir = src.join("csrc").join("build");
    println!("cargo:rustc-link-search=native={}", build_dir.display());
    println!("cargo:rustc-link-lib=static=lenet");
    // Runtime deps of the Lean runtime, per <lenet>/csrc/Makefile's check target.
    println!("cargo:rustc-link-lib=pthread");
    println!("cargo:rustc-link-lib=dl");
    println!("cargo:rustc-link-lib=m");
}

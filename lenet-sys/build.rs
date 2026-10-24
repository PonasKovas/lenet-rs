//! Build script: makes `lenet-sys` a normal, self-sufficient sys crate.
//!
//! By default it fetches the Lenet sources (a separate repository) at a
//! pinned revision into this crate's build directory, provisions the Lean
//! toolchain via elan if needed, and builds `liblenet.a` with the Lenet
//! repo's own Makefile. All you need is git, curl, cc/make, and a network.
//!
//! Local development against a Lenet checkout: set `LENET_DIR` to the repo
//! root. The build is incremental — editing Lenet sources triggers the
//! (fast) rebuild of just what changed.

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

const LENET_REPO: &str = "https://github.com/PonasKovas/lenet";
const LENET_REV: &str = "3197959c253620fe36321d26c27935902c88b347";

const ELAN_INIT: &str = "https://elan.lean-lang.org/elan-init.sh";

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

/// `git` invocation with output inherited (visible in cargo's build log).
/// `repo_arg`, when given, is passed to `-C`.
fn git(args: &[&str], repo_arg: &Path) {
    let mut cmd = Command::new("git");
    cmd.arg("-C").arg(repo_arg).args(args);
    run(&format!("git {} <lenet>", args.join(" ")), cmd);
}

/// Directories whose existence makes the Lean toolchain available without
/// installing anything: an elan install at `$HOME/.elan`, plus whatever is
/// already on PATH.
fn lean_search_dirs() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Some(home) = env::var_os("HOME") {
        dirs.push(PathBuf::from(home).join(".elan").join("bin"));
    }
    dirs
}

/// True if `name` is an existing file in one of the search dirs / PATH.
fn have_tool(name: &str) -> bool {
    let mut dirs = lean_search_dirs();
    if let Some(path) = env::var_os("PATH") {
        dirs.extend(env::split_paths(&path));
    }
    dirs.iter().any(|d| d.join(name).is_file())
}

/// PATH with the elan dir prepended, for spawned build commands.
fn path_with_elan() -> std::ffi::OsString {
    let mut path = env::split_paths(&env::var_os("PATH").unwrap_or_default()).collect::<Vec<_>>();
    for dir in lean_search_dirs().into_iter().rev() {
        path.insert(0, dir);
    }
    env::join_paths(&path).expect("PATH join")
}

/// Where the Lenet sources live: `LENET_DIR` (developer checkout) or a
/// pinned clone inside this build's OUT_DIR.
fn resolve_sources(out_dir: &Path) -> PathBuf {
    if let Some(dir) = env::var_os("LENET_DIR") {
        println!("cargo:rerun-if-env-changed=LENET_DIR");
        return PathBuf::from(dir);
    }

    let dir = out_dir.join("lenet");
    if !dir.join(".git").exists() {
        println!("cargo:warning=lenet-sys: fetching Lenet sources (pinned {LENET_REV})");
        let status = Command::new("git")
            .args(["clone", "--quiet", LENET_REPO])
            .arg(&dir)
            .status()
            .unwrap_or_else(|e| fail(&format!("failed to spawn git: {e} (is git installed?)")));
        if !status.success() {
            fail(&format!("git clone of {LENET_REPO} failed"));
        }
    }
    // Move to the pinned revision (fetching first if we don't have it).
    let at_rev = Command::new("git")
        .arg("-C")
        .arg(&dir)
        .args(["checkout", "--quiet"])
        .arg(LENET_REV)
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if !at_rev {
        git(&["fetch", "origin"], &dir);
        git(&["checkout", "--quiet", LENET_REV], &dir);
    }
    // Repair any local damage (interrupted clone, partial writes): the
    // working tree must exactly match the pinned revision.
    git(&["reset", "--hard", "--quiet", LENET_REV], &dir);

    // Sentinel check: a damaged checkout (e.g. zero-length files after a
    // crash) is wiped and re-cloned rather than trusted.
    let healthy = |d: &Path| {
        std::fs::metadata(d.join("lean-toolchain"))
            .map(|m| m.len() > 0)
            .unwrap_or(false)
            && std::fs::metadata(d.join("csrc").join("lenet_capi.c"))
                .map(|m| m.len() > 0)
                .unwrap_or(false)
    };
    if !healthy(&dir) {
        println!("cargo:warning=lenet-sys: Lenet checkout damaged; re-fetching");
        std::fs::remove_dir_all(&dir)
            .unwrap_or_else(|e| fail(&format!("cannot remove damaged clone: {e}")));
        let status = Command::new("git")
            .args(["clone", "--quiet", LENET_REPO])
            .arg(&dir)
            .status()
            .unwrap_or_else(|e| fail(&format!("failed to spawn git: {e}")));
        if !status.success() {
            fail(&format!("git clone of {LENET_REPO} failed"));
        }
        git(&["checkout", "--quiet", LENET_REV], &dir);
        git(&["reset", "--hard", "--quiet", LENET_REV], &dir);
        if !healthy(&dir) {
            fail("re-cloned Lenet checkout still looks damaged");
        }
    }
    dir
}

/// Emit rerun-if-changed for every Lenet source file (skipping build
/// outputs), so editing Lenet rebuilds exactly as needed.
fn track_sources(root: &Path) {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        println!("cargo:rerun-if-changed={}", dir.display());
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(_) => continue,
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

/// elan init (idempotent); skips when the toolchain is already available.
fn ensure_elan() {
    if have_tool("lake") {
        return;
    }
    println!("cargo:warning=lenet-sys: installing the Lean toolchain manager (elan)");
    run("elan install", {
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg(format!(
            "curl -sSfL {ELAN_INIT} | sh -s -- -y --default-toolchain none"
        ));
        cmd
    });
    if !have_tool("lake") {
        fail(
            "elan was installed but `lake` is still not available. \
             Install Lean manually (https://lean-lang.org/lean4/download/) \
             or set LENET_DIR to a Lenet checkout with a prebuilt \
             csrc/build/liblenet.a.",
        );
    }
}

/// Installs the exact toolchain Lenet pins. Idempotent: elan no-ops when
/// the toolchain is already present. `verbose` silences the progress note
/// on machines that already have a working Lean setup.
fn ensure_toolchain(src: &Path, verbose: bool) {
    let pinned = std::fs::read_to_string(src.join("lean-toolchain"))
        .unwrap_or_else(|e| fail(&format!("cannot read lean-toolchain: {e}")))
        .trim()
        .to_string();
    if verbose {
        println!("cargo:warning=lenet-sys: ensuring Lean toolchain {pinned} (no-op if cached)");
    }
    let mut cmd = Command::new("elan");
    cmd.args(["toolchain", "install", &pinned]);
    cmd.env("PATH", path_with_elan());
    run(&format!("elan toolchain install {pinned}"), cmd);
}

/// Prefix of the toolchain `lean-toolchain` names, as resolved by elan from
/// the Lenet repo root (`lean --print-prefix`).
fn toolchain_prefix(src: &Path) -> PathBuf {
    let out = Command::new("lean")
        .arg("--print-prefix")
        .current_dir(src)
        .env("PATH", path_with_elan())
        .output()
        .unwrap_or_else(|e| fail(&format!("failed to spawn `lean --print-prefix`: {e}")));
    if !out.status.success() {
        fail(&format!("`lean --print-prefix` failed with {}", out.status));
    }
    let prefix = String::from_utf8(out.stdout)
        .unwrap_or_else(|_| fail("`lean --print-prefix` printed non-UTF-8 output"));
    PathBuf::from(prefix.trim())
}

/// Builds `csrc/build/liblenet.a` with the Lenet repo's own Makefile
/// (incremental: a no-op when everything is current).
fn ensure_built(src: &Path) {
    let lib = src.join("csrc").join("build").join("liblenet.a");
    let have_lean = have_tool("lake");

    if lib.exists() && !have_lean {
        // Offline developer with a prebuilt checkout: link it as-is.
        return;
    }

    ensure_elan();
    ensure_toolchain(src, !have_lean);

    if !lib.exists() {
        println!("cargo:warning=lenet-sys: building Lenet (Lean compile; first build takes a few minutes)");
    }

    let mut cmd = Command::new("make");
    cmd.arg("-C").arg(src.join("csrc"));
    // Older Lenet Makefiles pick the first toolchain under ~/.elan, not the
    // pinned one; override it so the shim and runtime match what lake uses.
    cmd.arg(format!("TOOLCHAIN={}", toolchain_prefix(src).display()));
    cmd.env("PATH", path_with_elan());
    run("make -C <lenet>/csrc", cmd);

    if !lib.exists() {
        fail(&format!("make succeeded but {} is missing", lib.display()));
    }
}

fn main() {
    if cfg!(windows) {
        fail(
            "Windows is not supported: the Lean static runtime is built \
             without -fPIC TLS (shared libraries are impossible and the \
             build tooling assumes a POSIX shell). Use Linux or macOS.",
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

    track_sources(&src);
    ensure_built(&src);

    let build_dir = src.join("csrc").join("build");
    println!("cargo:rustc-link-search=native={}", build_dir.display());
    println!("cargo:rustc-link-lib=static=lenet");
    // Runtime deps of the Lean runtime, per <lenet>/csrc/Makefile's check target.
    println!("cargo:rustc-link-lib=pthread");
    println!("cargo:rustc-link-lib=dl");
    println!("cargo:rustc-link-lib=m");
}

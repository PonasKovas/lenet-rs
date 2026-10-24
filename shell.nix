# Dev shell: `nix-shell` (or `nix develop` with flakes enabled) before
# running cargo. Provides the tools the build scripts need beyond the
# base system: cmake for the enet-helper interop oracle (upstream ENet
# builds with it). Rust (rustup), Lean (elan), git, curl, and a C
# toolchain are expected from the host.
{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  packages = with pkgs; [
    cmake
  ];
}

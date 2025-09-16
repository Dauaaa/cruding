{
  inputs = {
    nixpkgs.url = "nixpkgs";
    flake-utils.url = "github:numtide/flake-utils";
    fenix.url = "github:nix-community/fenix";
  };

  outputs =
    {
      nixpkgs,
      flake-utils,
      fenix,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ fenix.overlays.default ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        rust-toolchain = pkgs.fenix.fromToolchainFile {
          file = ./rust-toolchain.toml;
          sha256 = "sha256-DjzLqHVKpOSw+W/aV7vPyqHsTeMvl/v234FPwNqr14s=";
        };

        libraries = [
          pkgs.libz
          pkgs.openssl
          pkgs.curl
        ];

      in
      {
        devShell = pkgs.mkShell {
            nativeBuildInputs = [
              pkgs.clang-tools
            ];

            packages = [
                rust-toolchain
                pkgs.nixfmt
                pkgs.fish
            ];

            shellHook = ''
              LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath libraries}:$LD_LIBRARY_PATH"
              RUST_SRC_PATH="${rust-toolchain}/lib/rustlib/src/rust/library"

              exec fish
            '';
          };
      }
    );
}

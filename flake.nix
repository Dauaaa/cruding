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
          sha256 = "sha256-Pyzt2MSYsRKWHi21uq4h1REVeN6/I2HTHNAf6wtGBa8=";
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
                pkgs.postgresql
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

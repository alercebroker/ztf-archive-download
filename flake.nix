{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-25.11";
    flake-utils.url = "github:numtide/flake-utils";
  };
  outputs =
    {
      nixpkgs,
      flake-utils,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true;
        };
        python = pkgs.python314;
        pythonEnv = python.withPackages (py: [
          py.ruff
          (py.debugpy.overridePythonAttrs { doCheck = false; })
          py.pytest
          py.uv-build
        ]);
      in
      {
        devShell = pkgs.mkShell rec {
          buildInputs = with pkgs; [
            stdenv.cc.cc.lib
            zlib
            snappy
            lz4
          ];
          packages = with pkgs; [
            rustc
            cargo
            pythonEnv
            uv
            basedpyright
            hyperfine
            duckdb
          ];

          # UV
          UV_PYTHON_PREFERENCE = "only-system";
          UV_PYTHON = "${python}";

          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath buildInputs;
        };
      }
    );
}

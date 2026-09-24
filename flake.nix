{
  description = "raft-db: fault-tolerant distributed DBMS in C17 using RAFT";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        # Benchmark notebooks (bench/notebooks/requirements.txt)
        python = pkgs.python3.withPackages (ps: with ps; [
          jupyter
          matplotlib
          pandas
        ]);
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            # Build (make -> cmake --preset, needs CMake >= 3.26)
            gnumake
            cmake
            gcc

            # Lint / format targets (make format, make iwyu)
            clang-tools
            include-what-you-use

            # Debugging
            gdb
            valgrind

            # ./run cluster launcher
            tmux

            # bench/ scripts
            wrk
            curl
            bc
            gawk

            python
          ];
        };

        formatter = pkgs.nixpkgs-fmt;
      });
}

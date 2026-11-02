{
  description = "Dashboard and tracker for personal finances";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
  };

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
      devShells = forAllSystems (pkgs:
        let
          # Pinned explicitly to match requires-python in pyproject.toml.
          python = pkgs.python313;
        in
        {
          default = pkgs.mkShell {
            packages = [
              python
              pkgs.uv
              pkgs.gnumake
              pkgs.git
              pkgs.openssh
            ];

            # Shared libraries that prebuilt wheels (numpy, scikit-learn...)
            # expect to find on a regular Linux system.
            LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
              pkgs.stdenv.cc.cc.lib
              pkgs.zlib
            ];

            shellHook = ''
              export PYTHONUNBUFFERED=1

              # Make uv build .venv from the Nix-provided interpreter instead of
              # downloading its own.
              export UV_PYTHON=${python}/bin/python
              export UV_PYTHON_DOWNLOADS=never
            '';
          };
        });
    };
}

{ pkgs ? import <nixpkgs> {} }:

let
  python = pkgs.python3;
in
pkgs.mkShell {
  buildInputs = [
    python
    pkgs.uv
  ];

  shellHook = ''
    export PYTHONUNBUFFERED=1
    export UV_SYSTEM_PYTHON=1
    export PYTHONPATH=$PWD/__pypackages__/${python.pythonVersion}/lib
    export PATH=$PWD/__pypackages__/${python.pythonVersion}/bin:$PATH

    # Tell uv to always use PEP 582 (not venv)
    export UV_PIP_TARGET=$PWD/__pypackages__/${python.pythonVersion}
  '';
}

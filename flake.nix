{
  description = "A basic flake with a shell";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  inputs.systems.url = "github:nix-systems/default";
  inputs.flake-utils = {
    url = "github:numtide/flake-utils";
    inputs.systems.follows = "systems";
  };

  outputs =
    { nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        devShells.default = pkgs.mkShell {
          packages = [
            # pkgs.texliveFull
            pkgs.azure-cli
            pkgs.gnumake
            pkgs.go
            pkgs.golangci-lint
            pkgs.kubectl
            pkgs.kubernetes-helm
            pkgs.nixfmt-rfc-style
            pkgs.opentofu
            pkgs.postgresql_17
            pkgs.tectonic
            pkgs.uv
          ];
        };
      }
    );
}

{
  description = "An empty flake template that you can adapt to your own environment";

  # Flake inputs
  inputs.nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0.1";

  # Flake outputs
  outputs =
    inputs:
    let
      # The systems supported for this flake
      supportedSystems = [
        "x86_64-linux" # 64-bit Intel/AMD Linux
        "aarch64-linux" # 64-bit ARM Linux
        "x86_64-darwin" # 64-bit Intel macOS
        "aarch64-darwin" # 64-bit ARM macOS
      ];

      # Helper to provide system-specific attributes
      forEachSupportedSystem =
        f:
        inputs.nixpkgs.lib.genAttrs supportedSystems (
          system:
          f {
            pkgs = import inputs.nixpkgs {
              inherit system;
              config.allowUnfree = true;
            };
            system = system;
          }
        );
    in
    {

      packages = forEachSupportedSystem (
        { pkgs, system }:
        {
          devEnv = pkgs.buildEnv {
            name = "bachelor-thesis-dev-env";
            paths = [
              pkgs.azure-cli
              pkgs.gnumake
              pkgs.kubectl
              pkgs.kubernetes-helm
              pkgs.nixfmt-rfc-style
              pkgs.opentofu
              pkgs.postgresql_17
              pkgs.tectonic
              pkgs.texliveFull
              pkgs.uv
            ];
          };
        }
      );

      devShells = forEachSupportedSystem (
        { pkgs, system }:
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              inputs.self.packages.${system}.devEnv
            ];
          };
        }
      );
    };
}

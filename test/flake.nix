{
  description = "A flake with required dependencies for the load-generator";

  inputs.nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0.1";

  outputs = inputs: let
    supportedSystems = [
      "x86_64-linux"
      "aarch64-linux"
      "x86_64-darwin"
      "aarch64-darwin"
    ];

    forEachSupportedSystem = f:
      inputs.nixpkgs.lib.genAttrs supportedSystems (system:
        f {
          pkgs = import inputs.nixpkgs {
            inherit system;
            config.allowUnfree = true;
          };
          system = system;
        });

  in {
    packages = forEachSupportedSystem (
      { pkgs, system }:
      {
        # buildEnv aggregates go and geos into one closure
        devEnv = pkgs.buildEnv {
          name = "bachelor-thesis-load-generator-dev-env";
          paths = [
            pkgs.go
            pkgs.geos
          ];
        };
      }
    );

    devShells = forEachSupportedSystem (
      { pkgs, system }:
      {
        # mkShell sets up environment variables so pkg-config can find geos
        default = pkgs.mkShell {
          buildInputs = [
            pkgs.go
            pkgs.geos
            pkgs.pkg-config
          ];
          shellHook = ''
            echo "Dev shell: pkg-config can now detect geos"
          '';
        };
      }
    );
  };
}

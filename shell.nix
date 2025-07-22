{
  pkgs ? import (builtins.fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/92c2e04a475523e723c67ef872d8037379073681.tar.gz";
    sha256 = "sha256-yLuz5cz5Z+sn8DRAfNkrd2Z1cV6DaYO9JMrEz4KZo/c=";
  }) { },
}:
pkgs.mkShell {
  packages = [
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
}

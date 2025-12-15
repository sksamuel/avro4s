{
  description = "Flake for dev shell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    sbtix.url = "github:natural-transformation/sbtix";
    gitignore = {
      url = "github:hercules-ci/gitignore.nix";
      # Use the same nixpkgs
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = inputs@{ nixpkgs, flake-parts, sbtix, gitignore, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" ];
      perSystem = { config, self', inputs', pkgs, system, ... }: 
      let
        jdk21-overlay = self: super: {
          jdk = super.jdk21;
          jre = super.jdk21;
          sbt = super.sbt.override { jre = super.jdk21; };
        };
        newPkgs = import nixpkgs {
          inherit system;
          overlays = [ jdk21-overlay ];
        };
        libPath = nixpkgs.lib.makeLibraryPath [ newPkgs.lmdb ];
        # Import the sbtix.nix file directly, not the flake's default.nix (which is a stub)
        sbtixPkg = newPkgs.callPackage "${sbtix}/plugin/nix-exprs/sbtix.nix" {};
        # Get the sbtix CLI tool from the flake for devShell
        sbtixCli = inputs'.sbtix.packages.sbtix;
      in {
        # Make packages.default lazy - wrap in a function that's only called when needed
        # This prevents it from being evaluated when only devShells is needed
        packages.default = (import ./default.nix { 
          pkgs = newPkgs; 
          gitignore = gitignore.lib;
          sbtix = sbtixPkg;
        });

        devShells.default = newPkgs.mkShell {
          nativeBuildInputs = with newPkgs; [
            sbt
            sbtixCli
            jdk
          ];
          # environment variables go here:
          # Set NIX_PATH to make sure the nix-build in `build.sh` produce the same results as `nix build`
          NIX_PATH = "nixpkgs=${inputs.nixpkgs}"; 
        };
      };
     
      flake = {
        # The usual flake attributes can be defined here, including system-
        # agnostic ones like nixosModule and system-enumerating ones, although
        # those are more easily expressed in perSystem.
      };
    };
}

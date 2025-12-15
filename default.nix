
# this file originates from SBTix
{ pkgs ? import <nixpkgs> {}
, cleanSource ? pkgs.lib.cleanSource
, gitignore ? (let
                  src = pkgs.fetchFromGitHub { 
                    owner = "hercules-ci";
                    repo = "gitignore.nix";
                    # put the latest commit sha of gitignore Nix library here:
                    rev = "9e21c80adf67ebcb077d75bd5e7d724d21eeafd6";
                    # use what nix suggests in the mismatch message here:
                    sha256 = "sha256:vky6VPK1n1od6vXbqzOXnekrQpTL4hbPAwUhT5J9c9E=";
                  };
                in import src { inherit (pkgs) lib; })
, sbtix
}:

let
  # sbtix is provided by flake.nix from the sbtix flake input
in
  sbtix.buildSbtLibrary {
    name = "avro4s";
    src = cleanSource (gitignore.gitignoreSource ./.);
    #sbtixBuildInputs = pkgs.callPackage ./sbtix-build-inputs.nix {};
    repo = [
      (import ./repo.nix)
      (import ./project/repo.nix)
      (import ./manual-repo.nix)
    ];
  }

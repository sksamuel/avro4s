
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
  #
  # Some Sbtix-generated repo locks (notably `project/project/repo.nix`) contain
  # sbt plugin artifacts under an *Ivy-style* layout (e.g.
  # `nix-sbt-plugin-releases/com.dwijnand/.../ivys/ivy.xml`), but declare the repo
  # pattern as empty (which the sbt launcher interprets as Maven layout).
  #
  # When building in the Nix sandbox (no network), this makes sbt fail to find
  # plugin dependencies like `com.dwijnand:sbt-compat`. We fix this by overriding
  # the repo pattern to the Ivy layout when we include `project/project/repo.nix`.
  ivyRepoPattern =
    "[organisation]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]";

  projectProjectRepo =
    let repoPath = ./project/project/repo.nix;
    in
      if builtins.pathExists repoPath then
        let repo = import repoPath;
        in repo // {
          repos = (repo.repos or {}) // {
            "nix-sbt-plugin-releases" = ivyRepoPattern;
          };
        }
      else
        {};
in
  sbtix.buildSbtLibrary {
    name = "avro4s";
    src = cleanSource (gitignore.gitignoreSource ./.);
    #sbtixBuildInputs = pkgs.callPackage ./sbtix-build-inputs.nix {};
    repo = [
      (import ./repo.nix)
      (import ./project/repo.nix)
      # Some sbt plugins (from project/plugins.sbt) end up locked under
      # project/project/repo.nix. Include it so Nix builds stay fully offline.
      projectProjectRepo
      (import ./manual-repo.nix)
    ];
  }

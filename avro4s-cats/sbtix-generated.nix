{ pkgs ? import <nixpkgs> {}
, cleanSource ? pkgs.lib.cleanSource
, gitignoreLib ? (let
                  src = pkgs.fetchFromGitHub {
                    owner = "hercules-ci";
                    repo = "gitignore.nix";
                    rev = "9e21c80adf67ebcb077d75bd5e7d724d21eeafd6";
                    sha256 = "sha256:vky6VPK1n1od6vXbqzOXnekrQpTL4hbPAwUhT5J9c9E=";
                  };
                in import src { inherit (pkgs) lib; })
}:

let
  sbtix = pkgs.callPackage ./sbtix.nix {};
  inherit (pkgs.lib) optional;

  sbtixSourceFetcher = { url, rev, narHash, sha256, ... }@args:
    if builtins ? fetchTree
    then builtins.fetchTree (builtins.removeAttrs args [ "sha256" ])
    else pkgs.fetchgit {
      inherit url rev sha256;
    };

  sbtixSource = sbtixSourceFetcher {
    type = "git";
    url = "https://github.com/natural-transformation/sbtix";
    rev = "cbc81e97fdb76fb56f86c54a84164427552d4c1b";
    narHash = "sha256-o0fIcmBuZaNxSrqGyG4SmcsTH0UXEG5rME3hyEwPcV0=";
    sha256 = "0pbi1x6ciqad61mnw40p8lgi7jwr29pci1ms99qs6rbfc1rchix3";
  };

  sbtixPluginRepos = [
    (import (sbtixSource + "/plugin/repo.nix"))
    (import (sbtixSource + "/plugin/project/repo.nix"))
    (import (sbtixSource + "/plugin/nix-exprs/manual-repo.nix"))
  ];

  sbtixPluginIvy = sbtix.buildSbtLibrary {
    name = "sbtix-plugin";
    src = cleanSource (sbtixSource + "/plugin");
    repo = sbtixPluginRepos;
  };

  pluginVersion =
    let versions = builtins.attrNames (builtins.readDir "${sbtixPluginIvy}/se.nullable.sbtix/sbtix/scala_2.12/sbt_1.0");
    in builtins.head versions;

  sbtixPluginJarPath = "${sbtixPluginIvy}/se.nullable.sbtix/sbtix/scala_2.12/sbt_1.0/${pluginVersion}/jars/sbtix.jar";


  manualRepo = import ./manual-repo.nix;
  repoLock = import ./repo.nix;
  projectRepo = import ./project/repo.nix;
  projectMetaRepoPath = ./project/project/repo.nix;
  projectMetaRepo =
    if builtins.pathExists projectMetaRepoPath
    then import projectMetaRepoPath
    else {};

  pluginRepoPath = ./sbtix-plugin-repo.nix;
  pluginRepo =
    if builtins.pathExists pluginRepoPath
    then import pluginRepoPath
    else null;

  repositories =
    [ repoLock projectRepo projectMetaRepo ]
    ++ optional (builtins.length (builtins.attrNames manualRepo.artifacts) > 0) manualRepo
    ++ optional (pluginRepo != null) pluginRepo;
  
  buildInputsPath = ./sbtix-build-inputs.nix;
  sbtixInputs =
    if builtins.pathExists buildInputsPath
    then pkgs.callPackage buildInputsPath {}
    else "";
in
  sbtix.buildSbtProgram {
    name = "avro4s-cats";
    src = cleanSource ./.;
    repo = repositories;
    sbtOptions = "-Dplugin.version=${pluginVersion}";
    sbtixBuildInputs = sbtixInputs;
    pluginBootstrap = ''
      pluginJar="${sbtixPluginJarPath}"

ivyDir="./.ivy2-home/local/se.nullable.sbtix/sbtix/scala_3/sbt_1.0/${pluginVersion}"
mkdir -p "$ivyDir/jars" "$ivyDir/ivys" "$ivyDir/poms"
if [ -n "${pluginJar:-}" ] && [ -f "$pluginJar" ]; then
  cp "$pluginJar" $ivyDir/jars/sbtix.jar
else
  echo "sbtix: unable to locate plugin jar; ensure SBTIX_SOURCE_URL/REV/NAR_HASH or SBTIX_PLUGIN_JAR_PATH are set." 1>&2
  exit 1
fi
  cat <<POM_EOF > $ivyDir/poms/sbtix-${pluginVersion}.pom
    <project xmlns="http://maven.apache.org/POM/4.0.0"
             xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
             xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
      <modelVersion>4.0.0</modelVersion>
      <groupId>se.nullable.sbtix</groupId>
      <artifactId>sbtix</artifactId>
      <version>${pluginVersion}</version>
      <name>sbtix Plugin</name>
      <description>Locally provided sbtix plugin for Nix build</description>
      <packaging>jar</packaging>
    </project>
POM_EOF
  cat <<IVY_EOF > $ivyDir/ivys/ivy.xml
    <ivy-module version="2.0" xmlns:e="http://ant.apache.org/ivy/extra">
      <info organisation="se.nullable.sbtix"
            module="sbtix"
            revision="${pluginVersion}"
            status="release"
            publication="1765836707662"
            e:sbtVersion="1.0"
            e:scalaVersion="2.12">
        <description>
          sbtix plugin (locally provided for Nix build)
        </description>
      </info>
      <configurations>
        <conf name="compile" visibility="public" description=""/>
        <conf name="default" visibility="public" description="" extends="compile"/>
        <conf name="master" visibility="public" description=""/>
        <conf name="provided" visibility="public" description=""/>
        <conf name="runtime" visibility="public" description="" extends="compile"/>
        <conf name="sources" visibility="public" description=""/>
        <conf name="test" visibility="public" description="" extends="runtime"/>
      </configurations>
      <publications>
        <artifact name="sbtix" type="jar" ext="jar" conf="compile"/>
      </publications>
      <dependencies></dependencies>
    </ivy-module>
IVY_EOF
ln -sf ivy.xml $ivyDir/ivys/ivy-${pluginVersion}.xml
    '';
  }

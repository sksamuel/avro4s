# This file originates from SBTix
{ runCommand, fetchurl, lib, stdenv, jdk, jre, sbt, writeText, makeWrapper, gawk
, extraPluginRepos ? []
}:
let
    inherit (lib)
      catAttrs
      concatLists
      concatMap
      concatStringsSep
      flatten
      fold
      init
      mapAttrsToList
      optionalString
      splitString
      toLower
      unique
      ;

    sbtTemplate = repoDefs: versioning:
    let
        buildSbt = writeText "build.sbt" ''
          scalaVersion := "${versioning.scalaVersion}"
        '';

        mainScala = writeText "Main.scala" ''
          object Main extends App {
            println("hello nix")
          }
        '';

        buildProperties = writeText "build.properties" ''
          sbt.version=${versioning.sbtVersion}
        '';

        # SBT Launcher Configuration
        # http://www.scala-sbt.org/0.13.5/docs/Launcher/Configuration.html
        sbtixRepos = writeText "sbt-setup-template-repos" ''
        [repositories]
        ${concatStringsSep "\n  " repoDefs}
        '';
    in stdenv.mkDerivation (rec {
            name = "sbt-setup-template";

            dontPatchELF      = true;
            dontStrip         = true;

            # set environment variable to affect all SBT commands
            SBT_OPTS = ''
             -Dsbt.ivy.home=./.ivy2/
             -Dsbt.boot.directory=./.sbt/boot/
             -Dsbt.global.base=./.sbt
             -Dsbt.global.staging=./.staging
             -Dsbt.override.build.repos=true
             -Dsbt.repository.config=${sbtixRepos}
            '';

            unpackPhase = ''
              runHook preUnpack

              ln -s ${buildSbt}  ./build.sbt
              ln -s ${mainScala} ./Main.scala

              mkdir -p ./project

              ln -s ${buildProperties} ./project/build.properties

              runHook postUnpack
            '';

            buildInputs = [ jdk sbt ];

            buildPhase = ''
              runHook preBuild

              sbt update

              runHook postBuild
            '';

            installPhase =''
              runHook preInstall

              mkdir -p $out
              # Copy the hidden ivy lock files. Only keep ivy cache folder, not ivy local. local might be empty now but I want to be sure it is not polluted in the future.
              rm -rf ./.ivy2/local
              cp -r --remove-destination ./.ivy2 $out/ivy
              cp -r --remove-destination ./.sbt $out/sbt

              runHook postInstall
            '';
    });

  mergeSbtTemplates = templates: runCommand "merge-sbt-template" {}
        (let
            copyTemplate = template:
                [ "cp -rns ${template}/ivy $out"
                  "cp -rns ${template}/sbt $out"
                  "chmod -R u+rw $out"
                ];
        in
            concatStringsSep "\n" (["mkdir -p $out"] ++ concatLists (map copyTemplate templates))
        );

  copyFile = name: input:
    runCommand name { inherit input; } ''
      cp -a $input $out
    '';

in rec {
    mkRepo = name: artifacts: localBuildsRepo: runCommand name {}
        (let
            parentDirs = filePath:
                concatStringsSep "/" (init (splitString "/" filePath));
            linkArtifact = outputPath: urlAttrs:
                let
                  artifact =
                    if urlAttrs.type or null == "built" then
                      if localBuildsRepo == "" then
                        abort "'sbtixBuildInputs' parameter missing from 'buildSbtProgram'/'buildSbtLibrary', but local dependencies found."
                      else
                        # Unlike fetched JARs which are content addressed derivations by virtue of being fixed-output,
                        # these copies are not content addressed, because ca-derivations is still experimental.
                        # This is unfortunate, because it will create duplication in the store during builds and development,
                        # but at least these can be GC-ed and allow the end result to only reference the single JARs that
                        # result from this copying operation.
                        copyFile (baseNameOf urlAttrs.path) (localBuildsRepo + "/" + urlAttrs.path)
                    else
                      # Use standard sha256 attribute
                      fetchurl { url = urlAttrs.url; sha256 = urlAttrs.sha256; };
                  hashBash =
                    if urlAttrs.type or null == "built"
                    then ''$(sha256sum "${artifact}" | cut -c -64)''
                    # Use sha256 directly
                    else ''$(echo ${toLower urlAttrs.sha256} | tr / _)'';

                  # Extract if this is a POM file
                  isPom = (builtins.match ".*\\.pom$" outputPath) != null;
                  
                  # For POM files, we need to create symlinks in both the parent directory and artifact path
                  pomLinks = if isPom then [
                    ''ln -fsn "${artifact}" "$out/${outputPath}"''
                  ] else [];
                in
                [ ''mkdir -p "$out/${parentDirs outputPath}"''
                  ''ln -fsn "${artifact}" "$out/${outputPath}"''
                  # TODO: include the executable bit as a suffix of the hash.
                  #       Shouldn't matter in our use case though.
                  ''ln -fsn "${artifact}" "$out/cas/${hashBash}"''
                ] ++ pomLinks;
        in
            ''
              mkdir -p $out/cas
            '' + concatStringsSep "\n" (concatLists (mapAttrsToList linkArtifact artifacts)));

    repoConfig = {repos, nixrepo, name}:
        let
            repoPatternOptional = repoPattern:
                optionalString (repoPattern != "") ", ${repoPattern}";
            
            # Add explicit pattern for Maven repos to handle POM files correctly
            mavenPattern = "[organization]/[module]/[revision]/[artifact]-[revision](-[classifier]).[ext]";
            
            repoPath = repoName: repoPattern:
                let 
                  # Use Maven pattern for public repos
                  pattern = if repoName == "public" 
                           then mavenPattern
                           else repoPattern;
                in
                [ "${name}-${repoName}: file://${nixrepo}/${repoName}/${repoPatternOptional pattern}" ];
        in
            concatLists (mapAttrsToList repoPath repos);

    ivyRepoPattern = "[organization]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]";

    mergeAttr = attr: repo:
        fold (a: b: a // b) {} (catAttrs attr repo);

    buildSbtProject = args@{repo, name, buildInputs ? [], sbtixBuildInputs ? "", sbtOptions ? "", pluginBootstrap ? "", ...}:
      let
          pluginRepoCandidates = [
            ./sbtix-plugin-repo.nix
            ../sbtix-plugin-repo.nix
          ];
          pluginRepos =
            concatLists (map
              (path: if builtins.pathExists path then [ (import path) ] else [])
              pluginRepoCandidates) ++ extraPluginRepos;
          mergedRepo = repo ++ pluginRepos;
          localBuildsRepo =
            if builtins.typeOf sbtixBuildInputs == "list" then
              abort "Update notice: see the sbtix README to learn how to use 'sbtixBuildInputs'"
            else
              sbtixBuildInputs;
          versionings = unique (flatten (catAttrs "versioning" mergedRepo));
          scalaVersion = (builtins.head versionings).scalaVersion;
          artifacts = mergeAttr "artifacts" mergedRepo;
          repos = mergeAttr "repos" mergedRepo;
          nixrepo = mkRepo "${name}-repo" artifacts localBuildsRepo;

          localRepoEntry = "local";
          remoteDefaults = [
            "maven-central"
            "typesafe-ivy-releases: https://repo.typesafe.com/typesafe/ivy-releases/, ${ivyRepoPattern}"
          ];
          repoDefs = (repoConfig { inherit repos nixrepo name; }) ++ (
            if (localBuildsRepo == "") then [] else [
              "sbtix-local-maven-dependencies: file://${localBuildsRepo}, ${mavenPattern}"
              "sbtix-local-ivy-dependencies: file://${localBuildsRepo}, ${ivyRepoPattern}"
            ]) ++ [ localRepoEntry ] ++ remoteDefaults;

          # This takes a couple of seconds, but only needs to run when dependencies have changed.
          # It's probably near-instantaneous if done in, say, python.
          combinedCas = runCommand "${name}-cas" {} ''
            mkdir -p $out/cas
            for casRef in ${nixrepo}/cas/*; do
              ln -sf "$(readlink "$casRef")" $out/cas/$(basename "$casRef")
            done
          '';


          # SBT Launcher Configuration
          # http://www.scala-sbt.org/0.13.5/docs/Launcher/Configuration.html
          sbtixRepos = writeText "${name}-repos" ''
            [repositories]
              ${concatStringsSep "\n  " repoDefs}
            '';
            
          # Define Maven pattern for consistency
          mavenPattern = "[organization]/[module]/[revision]/[artifact]-[revision](-[classifier]).[ext]";

      in stdenv.mkDerivation (rec {
            dontPatchELF      = true;
            dontStrip         = true;
            doDist            = true;

            # COURSIER_CACHE env variable is needed if one wants to use non-sbtix repositories in the below repo list, which is sometimes useful.
            COURSIER_CACHE = "./.cache/coursier/v1";

            # Marker for sbt builds running under the Nix sandbox.
            #
            # Some projects use this to disable tasks that would otherwise try to
            # download tools at build time (e.g. scalafmt downloads its core jars).
            SBTIX_NIX_BUILD = "1";

            # set environment variable to affect all SBT commands
            SBT_OPTS = ''
              -Dsbt.ivy.home=./.ivy2/
              -Dsbt.boot.directory=./.sbt-boot
              -Dsbt.global.base=./.sbt
              -Dsbt.global.staging=./.staging
              -Dsbt.override.build.repos=true
              -Dsbt.repository.config=${sbtixRepos}
              -Dsbt.offline=true
              -Dsbtix.nixBuild=true
              ${sbtOptions}
            '';

            buildPhase = ''
              runHook preBuild

              export COURSIER_CACHE=$(pwd)/.coursier-cache
              baseSbtOpts="''${SBT_OPTS:-}"
              export SBT_OPTS="$baseSbtOpts ${sbtOptions} -Dsbt.ivy.home=./.ivy2-home -Dsbt.boot.directory=./.sbt-boot -Dcoursier.cache=./.coursier-cache"
              localHome="$(pwd)/.sbt-home"
              localCache="$localHome/.cache"
              mkdir -p "$localHome" "$localCache"
              export SBT_OPTS="$SBT_OPTS -Duser.home=$localHome"
              export HOME="$localHome"
              export XDG_CACHE_HOME="$localCache"

              # Setup local directory for plugin
              ${pluginBootstrap}

              if [ -d ${sbt}/share/sbt/boot ]; then
                echo "[SBTIX_NIX_DEBUG] Seeding sbt boot from ${sbt}/share/sbt/boot"
                mkdir -p ./.sbt-boot
                cp -RL ${sbt}/share/sbt/boot/. ./.sbt-boot/
                chmod -R u+w ./.sbt-boot || true
              fi

              echo "[SBTIX_NIX_DEBUG] localBuildsRepo='${localBuildsRepo}'"
              if [ -n "${localBuildsRepo}" ]; then
                mkdir -p ./.ivy2-home/local
                cp -RL ${localBuildsRepo}/. ./.ivy2-home/local/
                chmod -R u+w ./.ivy2-home/local
                echo "[SBTIX_NIX_DEBUG] Contents of ./.ivy2-home/local (depth 3):"
                find ./.ivy2-home/local -maxdepth 3 -mindepth 1 -print
              else
                echo "[SBTIX_NIX_DEBUG] No localBuildsRepo provided"
              fi

              echo "[SBTIX_NIX_DEBUG] sbtixRepos configuration:"
              cat ${sbtixRepos}

              sbt compile
              runHook postBuild
            '';

            distPhase = ''
              runHook preDist

              echo 1>&2 "replacing copies by references..."
              saved=0
              while read file; do
                hash="$(sha256sum "$file" | cut -c -64 | tr / _)"
                entry="${combinedCas}/cas/$hash"
                if [[ -e $entry ]]; then
                  size="$(stat -c%s $file)"
                  echo 1>&2 "replacing $file ($size bytes)"
                  saved=$[saved+size]
                  rm $file
                  ln -s "$(readlink "$entry")" "$file"
                fi
              done < <(find $out -type f)
              echo 1>&2 "saved $[saved/1000] kB"

              runHook postDist
            '';

            # These inputs are only meant for the build process. If they stick
            # around in the outputs, they'd just bloat the user package for no
            # good reason.
            disallowedReferences = [
              combinedCas
              sbtixRepos
              nixrepo
            ]
            ++ lib.optional (localBuildsRepo != "") [
              localBuildsRepo
            ];
        } // args // {
            repo = null;
            buildInputs = [ makeWrapper jdk sbt ] ++ buildInputs;
        });

    buildSbtLibrary = args@{repo, ...}:
      let
        mainRepo = builtins.head repo;
        scalaVersion = (builtins.head mainRepo.versioning).scalaVersion;
      in buildSbtProject ({
        installPhase = ''
          runHook preInstall

          localHome="$(pwd)/.sbt-home"
          localCache="$localHome/.cache"
          mkdir -p "$localHome" "$localCache"
          if [ -d ${sbt}/share/sbt/boot ]; then
            echo "[SBTIX_NIX_DEBUG] Seeding sbt boot from ${sbt}/share/sbt/boot"
            mkdir -p ./.sbt-boot
            cp -RL ${sbt}/share/sbt/boot/. ./.sbt-boot/
            chmod -R u+w ./.sbt-boot || true
          fi
          export SBT_OPTS="''${SBT_OPTS:-} -Duser.home=$localHome"
          HOME="$localHome" XDG_CACHE_HOME="$localCache" sbt ++${scalaVersion} publishLocal
          mkdir -p $out/
          localIvyDir="./.ivy2-home/local"
          if [ -d "$localIvyDir" ]; then
            cp -r "$localIvyDir/." $out/
          else
            echo "warning: expected Ivy cache at $localIvyDir but it was not created" 1>&2
          fi

          runHook postInstall
        '';
    } // args);

    buildSbtProgram = args: buildSbtProject ({
        installPhase = ''
          runHook preInstall

          localHome="$(pwd)/.sbt-home"
          localCache="$localHome/.cache"
          mkdir -p "$localHome" "$localCache"
          if [ -d ${sbt}/share/sbt/boot ]; then
            echo "[SBTIX_NIX_DEBUG] Seeding sbt boot from ${sbt}/share/sbt/boot"
            mkdir -p ./.sbt-boot
            cp -RL ${sbt}/share/sbt/boot/. ./.sbt-boot/
            chmod -R u+w ./.sbt-boot || true
          fi
          export SBT_OPTS="''${SBT_OPTS:-} -Duser.home=$localHome"
          HOME="$localHome" XDG_CACHE_HOME="$localCache" sbt stage
          mkdir -p $out/
          copied_any=0
          copy_stage_root() {
            local dir="$1"
            if [ -d "$dir" ]; then
              echo "[SBTIX_NIX_DEBUG] Copying staged application from $dir"
              cp -r "$dir"/. $out/
              copied_any=1
            fi
          }

          copy_stage_root "target/universal/stage"
          while IFS= read -r stageDir; do
            if [ -n "$stageDir" ] && [ "$stageDir" != "target/universal/stage" ]; then
              copy_stage_root "$stageDir"
            fi
          done < <(find . -path '*/target/universal/stage' -type d)

          if [ $copied_any -eq 0 ]; then
            echo "error: no staged artifacts found. Ensure sbt-native-packager is enabled or override sbtix.buildSbtProgram." 1>&2
            exit 1
          fi
          for p in $(find $out/bin/* -executable); do
            wrapProgram "$p" --prefix PATH : ${jre}/bin --prefix PATH : ${gawk}/bin
          done

          runHook postInstall
        '';
    } // args);
}

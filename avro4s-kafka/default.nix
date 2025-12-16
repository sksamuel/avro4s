{ pkgs ? import <nixpkgs> {} }:
let
  sbtixGenerated = import ./sbtix-generated.nix { inherit pkgs; };
in sbtixGenerated

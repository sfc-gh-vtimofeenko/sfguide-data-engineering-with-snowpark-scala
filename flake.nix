{
  description = "Development environment for Data Engineering Pipelines with Snowpark Scala";

  inputs = {
    # NOTE: transitive inputs are not pinned to ensure reproducibility at the cost of disk space
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    # nixpkgs-lib.url = "github:NixOS/nixpkgs/nixos-unstable?dir=lib";
    snowcli.url = "github:sfc-gh-vtimofeenko/snowcli?ref=nix-flake&dir=contrib/nix";
    scala-seed.url = "github:devinsideyou/scala-seed/67430de28c463fe2c864c6b7a9bc4ee5fa3ebf73";
    treefmt-nix.url = "github:numtide/treefmt-nix";
    # TODO: some sort of motd mergeable with scala-seed devshell, like mission-control?
    # devshell.url = "github:numtide/devshell";
  };

  outputs = inputs@{ flake-parts, ... }:
    # let
    #   inherit (inputs.nixpkgs-lib) lib;
    # in
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [ inputs.treefmt-nix.flakeModule ];
      systems = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" "x86_64-darwin" ];
      perSystem = { config, self', inputs', pkgs, system, ... }: {
        devShells.default = inputs'.scala-seed.devShells.java11.overrideAttrs (prev:
          /* add snowcli connection wrapper with .envrc values */
          let
            snowCliWrapped = pkgs.writeShellApplication {
              name = "snow";
              runtimeInputs = [ inputs'.snowcli.packages.default ];
              text = ''
                snow \
                --config-file <(cat<<EOF
                [connections]
                [connections.dev] # DEFAULT connection name is this
                account = '$SNOWSQL_ACCOUNT'
                user = '$SNOWSQL_USER'
                database = '$SNOWSQL_DATABASE'
                schema = '$SNOWSQL_SCHEMA'
                password = '$SNOWSQL_PWD'
                EOF
                ) \
                "$@";
              '';
            };
          in
          {
            buildInputs = prev.buildInputs ++ [ snowCliWrapped pkgs.scalafix pkgs.maven ];
          })
        ;
        treefmt.programs = {
          nixpkgs-fmt.enable = true;
          scalafmt.enable = true;
        };
        treefmt.projectRootFile = "flake.nix";
      };
      flake = { };
    };
}

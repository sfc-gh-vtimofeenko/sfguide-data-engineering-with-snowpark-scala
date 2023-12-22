{
  description = "Development environment for Data Engineering Pipelines with Snowpark Scala";

  inputs = {
    # NOTE: transitive inputs are not pinned to ensure reproducibility at the cost of disk space
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-23.11";
    home-manager.url = "github:rycee/home-manager/release-23.11";
    home-manager.inputs.nixpkgs.follows = "nixpkgs";
    # nixpkgs-lib.url = "github:NixOS/nixpkgs/nixos-unstable?dir=lib";
    snowcli.url = "github:sfc-gh-vtimofeenko/snowcli?ref=nix-flake&dir=contrib/nix";
    devshell.url = "github:numtide/devshell";
    treefmt-nix.url = "github:numtide/treefmt-nix";
  };

  outputs = inputs@{ flake-parts, ... }:
    # let
    #   inherit (inputs.nixpkgs-lib) lib;
    # in
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [ inputs.treefmt-nix.flakeModule inputs.devshell.flakeModule ];
      systems = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" "x86_64-darwin" ];
      perSystem = { config, self', inputs', pkgs, system, ... }: {
        _module.args.pkgs = import inputs.nixpkgs {
          inherit system;
          overlays = [
            (final: prev: {
              jdk = pkgs.temurin-bin-11;
              jre = pkgs.temurin-bin-11;
            })
          ];
          config = { };
        };

        legacyPackages.homeConfigurations.vscode = inputs.home-manager.lib.homeManagerConfiguration ({
          inherit pkgs;
          modules = [
            {
              home.stateVersion = "22.11";
              home.username = "vscode";
              home.homeDirectory = "/home/vscode";
              home.sessionVariables = {
                JAVA_HOME = pkgs.temurin-bin-11;
              };
              home.packages = [
                pkgs.jc
                pkgs.jq
              ];
              programs.direnv = {
                enable = true;
                nix-direnv.enable = true;
                config = {
                  whitelist = {
                    prefix = [ "/workspaces" ];
                  };
                };
              };

            }
          ];
        }
        );


        devshells.default =
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
            env = [
              {
                name = "JAVA_HOME";
                value = pkgs.temurin-bin-11;
              }
            ];
            commands = [ ];
            packages = [
              snowCliWrapped
              pkgs.maven
              pkgs.jc
              pkgs.jq
              pkgs.sbt
            ];
          };

        treefmt.programs = {
          nixpkgs-fmt.enable = true;
          scalafmt.enable = true;
        };
        treefmt.projectRootFile = "flake.nix";
      };
      flake = { };
    };
}

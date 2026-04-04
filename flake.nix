{
  description = "A python dev starter package.";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    # nix flake lock --override-input nixpkgs "github:NixOS/nixpkgs?rev=fa83fd837f3098e3e678e6cf017b2b36102c7211"

    flake-parts.url = "github:hercules-ci/flake-parts";
    flake-parts.inputs.nixpkgs-lib.follows = "nixpkgs";
    
    nixos-pkgs.url = "git+ssh://git@github.com/liuhh666233/nixos-pkgs";
    nixos-pkgs.inputs.nixpkgs.follows = "nixpkgs";
    nixos-pkgs.inputs.flake-parts.follows = "flake-parts";

  };

  outputs = { self, nixpkgs, flake-parts, ... }@inputs:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" "aarch64-darwin" ];

      imports = [ ./nix/development.nix ];

      perSystem = { system, config, pkgs-dev, ... }: {
        # With this, you can run `nix fmt` to format all nix files in this repo.
        formatter = pkgs-dev.nixfmt-classic;
      };
    };
}

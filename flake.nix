{
  description = "Datalox - A Datalog implementation in Elixir";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        # Latest Erlang/OTP and Elixir
        erlang = pkgs.erlang_27;
        elixir = pkgs.beam.packages.erlang_27.elixir_1_18;
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = [
            erlang
            elixir
            pkgs.git
          ];

          shellHook = ''
            echo "Datalox Development Environment"
            echo "Erlang/OTP: $(erl -eval 'erlang:display(erlang:system_info(otp_release)), halt().' -noshell)"
            echo "Elixir: $(elixir --version | head -n 1)"

            # Set up Mix environment
            export MIX_HOME="$PWD/.nix-mix"
            export HEX_HOME="$PWD/.nix-hex"
            export PATH="$MIX_HOME/bin:$HEX_HOME/bin:$PATH"
            export ERL_AFLAGS="-kernel shell_history enabled"
          '';
        };
      }
    );
}

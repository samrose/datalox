defmodule Datalox.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/samrose/datalox"

  def project do
    [
      app: :datalox,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      package: package(),
      name: "Datalox",
      description: "A Datalog implementation in Elixir for rule engines",
      source_url: @source_url,
      dialyzer: [
        plt_add_apps: [:mix, :ex_unit],
        plt_file: {:no_warn, "priv/plts/dialyzer.plt"}
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Datalox.Application, []}
    ]
  end

  defp deps do
    [
      {:nimble_parsec, "~> 1.4"},
      {:telemetry, "~> 1.2"},
      {:stream_data, "~> 1.0", only: [:test, :dev]},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false}
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "LICENSE"],
      source_url: @source_url
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
    ]
  end
end

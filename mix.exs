defmodule Oban.Pro.MixProject do
  use Mix.Project

  @version "0.12.9"

  def project do
    [
      app: :oban_pro,
      version: @version,
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      docs: docs(),
      aliases: aliases(),
      package: package(),
      name: "Oban Pro",
      description: "Oban Pro Component",
      preferred_cli_env: [
        "test.ci": :test,
        "test.reset": :test,
        "test.setup": :test
      ],

      # Exclude Graph checks, as it is an optional dependency
      xref: [exclude: [Graph, Oban.Validation]],

      # Dialyzer
      dialyzer: [
        plt_add_apps: [:ex_unit, :libgraph, :mix],
        plt_core_path: "_build/#{Mix.env()}",
        flags: [:error_handling, :underspecs]
      ]
    ]
  end

  def application do
    [
      mod: {Oban.Pro.Application, []},
      extra_applications: [:logger]
    ]
  end

  def package do
    [
      organization: "oban",
      files: ~w(lib/pro.ex lib/oban .formatter.exs mix.exs),
      licenses: ["Commercial"],
      links: []
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp deps do
    [
      {:oban, "~> 2.13"},
      {:ecto_sql, "~> 3.8"},
      {:libgraph, "~> 0.13", optional: true},
      {:stream_data, "~> 0.5", only: [:test, :dev]},
      {:tzdata, "~> 1.0", only: [:test, :dev]},
      {:benchee, "~> 1.0", only: [:test, :dev], runtime: false},
      {:credo, "~> 1.6", only: [:test, :dev], runtime: false},
      {:dialyxir, "~> 1.0", only: [:test, :dev], runtime: false},
      {:ex_doc, "~> 0.21", only: [:dev], runtime: false},
      {:makeup_diff, "~> 0.1", only: [:dev], runtime: false},
      {:lys_publish, "~> 0.1", only: [:dev], path: "../lys_publish", runtime: false}
    ]
  end

  defp aliases do
    [
      release: [
        "cmd git tag v#{@version}",
        "cmd git push",
        "cmd git push --tags",
        "hex.publish package --yes",
        "lys.publish"
      ],
      "test.reset": ["ecto.drop -r Oban.Pro.Repo", "test.setup"],
      "test.setup": ["ecto.create -r Oban.Pro.Repo --quiet", "ecto.migrate -r Oban.Pro.Repo"],
      "test.ci": [
        "format --check-formatted",
        "deps.unlock --check-unused",
        "credo --strict",
        "test --raise",
        "dialyzer"
      ]
    ]
  end

  defp docs do
    [
      main: "overview",
      source_ref: "v#{@version}",
      formatters: ["html"],
      api_reference: false,
      extra_section: "GUIDES",
      extras: extras(),
      groups_for_extras: groups_for_extras(),
      groups_for_modules: groups_for_modules(),
      homepage_url: "/",
      skip_undefined_reference_warnings_on: ["CHANGELOG.md"],
      before_closing_body_tag: fn _ ->
        """
        <script>document.querySelector('footer.footer p').remove()</script>
        """
      end
    ]
  end

  defp extras do
    [
      "guides/introduction/overview.md",
      "guides/introduction/installation.md",
      "guides/introduction/adoption.md",
      "guides/extensions/smart_engine.md",
      "guides/extensions/worker.md",
      "guides/plugins/dynamic_cron.md",
      "guides/plugins/dynamic_lifeline.md",
      "guides/plugins/dynamic_queues.md",
      "guides/plugins/reprioritizer.md",
      "guides/testing/testing.md",
      "guides/testing/testing_workers.md",
      "guides/deployment/docker.md",
      "guides/deployment/gigalixir.md",
      "guides/deployment/heroku.md",
      "CHANGELOG.md": [filename: "changelog", title: "Changelog"]
    ]
  end

  defp groups_for_extras do
    [
      Introduction: ~r/guides\/introduction\/.?/,
      Extensions: ~r/guides\/extensions\/.?/,
      Plugins: ~r/guides\/plugins\/.?/,
      Workers: ~r/guides\/workers\/.?/,
      Testing: ~r/guides\/testing\/.?/,
      Deployment: ~r/guides\/deployment\/.?/
    ]
  end

  defp groups_for_modules do
    [
      Extensions: [
        Oban.Pro.Relay,
        Oban.Pro.Worker
      ],
      Plugins: [
        Oban.Pro.Plugins.DynamicPruner
      ],
      Testing: [
        Oban.Pro.Testing
      ],
      Workers: [
        Oban.Pro.Workers.Batch,
        Oban.Pro.Workers.Chunk,
        Oban.Pro.Workers.Workflow
      ]
    ]
  end
end

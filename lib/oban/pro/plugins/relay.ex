defmodule Oban.Pro.Plugins.Relay do
  @moduledoc false

  @behaviour Oban.Plugin

  use GenServer

  alias Oban.Pro.Relay

  @impl Oban.Plugin
  def start_link(_opts) do
    IO.warn("Relay no longer requires a plugin, you can remove it from your plugins list")

    :ignore
  end

  @impl Oban.Plugin
  def validate(_opts), do: :ok

  @impl GenServer
  def init(_opts), do: :ignore

  defdelegate async(name \\ Oban, changeset), to: Relay

  defdelegate await(relay, timeout \\ 5_000), to: Relay

  defdelegate await_many(relays, timeout \\ 5_000), to: Relay
end

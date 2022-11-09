defmodule Oban.Pro.Plugins.BatchManager do
  @moduledoc false

  @behaviour Oban.Plugin

  use GenServer

  @impl Oban.Plugin
  def start_link(_opts) do
    IO.warn("Batches no longer require a manager, you can remove it from your plugins list")

    :ignore
  end

  @impl Oban.Plugin
  def validate(_opts), do: :ok

  @impl GenServer
  def init(_opts), do: :ignore
end

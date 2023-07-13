defmodule Oban.Pro.Plugins.Reprioritizer do
  @moduledoc false

  @behaviour Oban.Plugin

  use GenServer

  alias Oban.Pro.Plugins.DynamicPrioritizer

  @impl Oban.Plugin
  def start_link(opts) do
    IO.warn("Reprioritizer is deprecated, use DynamicPrioritizer instead")

    DynamicPrioritizer.start_link(opts)
  end

  @impl GenServer
  def init(_opts), do: :ignore

  @impl Oban.Plugin
  def validate(_opts), do: :ok
end

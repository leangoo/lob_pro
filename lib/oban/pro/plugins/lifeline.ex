defmodule Oban.Pro.Plugins.Lifeline do
  @moduledoc false

  use GenServer

  alias Oban.Pro.Plugins.DynamicLifeline

  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts) do
    IO.warn("The Lifeline plugin is deprecated, use DynamicLifeline instead")

    DynamicLifeline.start_link(opts)
  end

  @doc false
  def init(_opts), do: :ignore
end

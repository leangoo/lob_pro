defmodule Oban.Pro.Notifiers.Pg do
  @moduledoc false

  alias Oban.Notifiers.PG

  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts) do
    IO.warn("The PG notifier was moved to Oban, use Oban.Notifiers.PG instead")

    PG.start_link(opts)
  end

  defdelegate listen(server, channels), to: PG

  defdelegate unlisten(server, channels), to: PG

  defdelegate notify(server, channel, payload), to: PG
end

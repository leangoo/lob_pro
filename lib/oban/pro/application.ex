defmodule Oban.Pro.Application do
  @moduledoc false

  use Application

  @handlers [Oban.Pro.Relay, Oban.Pro.Worker, Oban.Pro.Workers.Batch]

  @impl Application
  def start(_type, _args) do
    for handler <- @handlers, do: handler.on_start()

    Supervisor.start_link([], strategy: :one_for_one, name: __MODULE__)
  end

  @impl Application
  def stop(_state) do
    for handler <- @handlers, do: handler.on_stop()
  end
end

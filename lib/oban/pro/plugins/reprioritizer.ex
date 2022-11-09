defmodule Oban.Pro.Plugins.Reprioritizer do
  @moduledoc false

  @behaviour Oban.Plugin

  use GenServer

  import Ecto.Query, only: [where: 3]

  alias Oban.{Job, Peer, Repo, Validation}

  defmodule State do
    @moduledoc false

    defstruct [
      :conf,
      :name,
      :timer,
      interval: :timer.seconds(60),
      after: :timer.minutes(5),
      queue_overrides: [],
      worker_overrides: []
    ]
  end

  @impl Oban.Plugin
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  @impl Oban.Plugin
  def validate(opts) do
    Validation.validate(opts, fn
      {:conf, _} -> :ok
      {:name, _} -> :ok
      {:after, period} -> Validation.validate_timeout(:after, period)
      {:interval, interval} -> Validation.validate_integer(:interval, interval)
      {:queue_overrides, overrides} -> validate_overrides(:queue, overrides)
      {:worker_overrides, overrides} -> validate_overrides(:worker, overrides)
      option -> {:error, "unknown option provided: #{inspect(option)}"}
    end)
  end

  # Callbacks

  @impl GenServer
  def init(opts) do
    Validation.validate!(opts, &validate/1)

    Process.flag(:trap_exit, true)

    state =
      State
      |> struct!(opts)
      |> schedule_reprioritization()

    :telemetry.execute([:oban, :plugin, :init], %{}, %{conf: state.conf, plugin: __MODULE__})

    {:ok, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    if is_reference(state.timer), do: Process.cancel_timer(state.timer)

    :ok
  end

  @impl GenServer
  def handle_info(:reprioritize, state) do
    meta = %{conf: state.conf, plugin: __MODULE__}

    :telemetry.span([:oban, :plugin], meta, fn ->
      case reprioritize_starved_jobs(state) do
        {:ok, count} when is_integer(count) ->
          {:ok, Map.put(meta, :reprioritized_count, count)}

        error ->
          {:error, Map.put(meta, :error, error)}
      end
    end)

    {:noreply, schedule_reprioritization(state)}
  end

  # Validation

  defp validate_overrides(parent_key, overrides) do
    Validation.validate(parent_key, overrides, &validate_override/1)
  end

  defp validate_override({key, value}) when is_atom(key) do
    Validation.validate_timeout(key, value)
  end

  defp validate_override(option) do
    {:error, "expected override option to be a tuple, got: #{inspect(option)}"}
  end

  # Scheduling

  defp schedule_reprioritization(state) do
    %{state | timer: Process.send_after(self(), :reprioritize, state.interval)}
  end

  # Queries

  defp reprioritize_starved_jobs(state) do
    if Peer.leader?(state.conf) do
      Repo.transaction(state.conf, fn ->
        queue_counts =
          for {queue, period} <- state.queue_overrides do
            base_query()
            |> query_for_queues(:any, [to_string(queue)])
            |> query_for_period(period)
            |> update_all(state)
          end

        worker_counts =
          for {worker, period} <- state.worker_overrides do
            base_query()
            |> query_for_workers(:any, [to_string(worker)])
            |> query_for_period(period)
            |> update_all(state)
          end

        default_count =
          base_query()
          |> query_for_queues(:not, string_keys(state.queue_overrides))
          |> query_for_workers(:not, string_keys(state.worker_overrides))
          |> query_for_period(state.after)
          |> update_all(state)

        [queue_counts, worker_counts]
        |> List.flatten()
        |> Enum.reduce(default_count, &(&1 + &2))
      end)
    else
      {:ok, 0}
    end
  end

  defp string_keys(keyword) do
    for {key, _} <- keyword, do: to_string(key)
  end

  defp base_query do
    Job
    |> where([j], j.state == "available")
    |> where([j], j.priority > 0)
  end

  defp query_for_period(query, :infinity) do
    where(query, [j], true == false)
  end

  defp query_for_period(query, period) do
    where(query, [j], j.scheduled_at <= ^to_timestamp(period))
  end

  defp query_for_queues(query, :not, []), do: query
  defp query_for_queues(query, :not, queues), do: where(query, [j], j.queue not in ^queues)
  defp query_for_queues(query, :any, queues), do: where(query, [j], j.queue in ^queues)

  defp query_for_workers(query, :not, []), do: query
  defp query_for_workers(query, :not, workers), do: where(query, [j], j.worker not in ^workers)
  defp query_for_workers(query, :any, workers), do: where(query, [j], j.worker in ^workers)

  defp update_all(query, state) do
    state.conf
    |> Repo.update_all(query, inc: [priority: -1])
    |> elem(0)
  end

  defp to_timestamp(milliseconds) do
    DateTime.add(DateTime.utc_now(), -milliseconds, :millisecond)
  end
end

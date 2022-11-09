defmodule Oban.Pro.Plugins.DynamicLifeline do
  @moduledoc false

  @behaviour Oban.Plugin

  use GenServer

  import Ecto.Query, only: [join: 5, select: 2, where: 3]

  alias Oban.Pro.Producer
  alias Oban.Pro.Queue.SmartEngine
  alias Oban.{Job, Peer, Repo, Validation}

  defmodule State do
    @moduledoc false

    defstruct [
      :conf,
      :name,
      :rescue_timer,
      retry_exhausted: false,
      rescue_interval: :timer.minutes(1)
    ]
  end

  @impl Oban.Plugin
  def start_link(opts) do
    case opts[:conf] do
      %{engine: SmartEngine} ->
        GenServer.start_link(__MODULE__, opts, name: opts[:name])

      %{engine: engine} ->
        raise RuntimeError, """
        The DynamicLifeline plugin requires the SmartEngine to run correctly, but you're using:

        engine: #{inspect(engine)}

        You can either switch to use the SmartEngine or remove DynamicLifeline from your plugins.
        """
    end
  end

  @impl Oban.Plugin
  def validate(opts) do
    Validation.validate(opts, fn
      {:conf, _} -> :ok
      {:name, _} -> :ok
      {:rescue_interval, interval} -> Validation.validate_integer(:rescue_interval, interval)
      {:rescue_after, interval} -> Validation.validate_integer(:rescue_after, interval)
      {:retry_exhausted, boolean} -> validate_boolean(:retry_exhausted, boolean)
      option -> {:error, "unknown option provided: #{inspect(option)}"}
    end)
  end

  @impl GenServer
  def init(opts) do
    Validation.validate!(opts, &validate/1)

    Process.flag(:trap_exit, true)

    state =
      State
      |> struct!(opts)
      |> schedule_rescue()

    :telemetry.execute([:oban, :plugin, :init], %{}, %{conf: state.conf, plugin: __MODULE__})

    {:ok, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    if is_reference(state.rescue_timer), do: Process.cancel_timer(state.rescue_timer)

    :ok
  end

  @impl GenServer
  def handle_info(:rescue, state) do
    meta = %{conf: state.conf, plugin: __MODULE__}

    :telemetry.span([:oban, :plugin], meta, fn ->
      rescued_count =
        if Peer.leader?(state.conf) do
          rescue_orphaned(state)
        else
          0
        end

      stop_meta =
        meta
        |> Map.put(:deleted_count, 0)
        |> Map.put(:rescued_count, rescued_count)

      {:ok, stop_meta}
    end)

    {:noreply, schedule_rescue(state)}
  end

  # Validation

  defp validate_boolean(key, value) do
    if is_boolean(value) do
      :ok
    else
      {:error, "expected #{inspect(key)} to be a boolean, got: #{inspect(value)}"}
    end
  end

  # Scheduling

  defp schedule_rescue(state) do
    timer = Process.send_after(self(), :rescue, state.rescue_interval)

    %{state | rescue_timer: timer}
  end

  # Queries

  defp rescue_orphaned(state) do
    subquery =
      Job
      |> where([j], not is_nil(j.queue) and j.state == "executing")
      |> join(:left, [j], p in Producer,
        on:
          fragment("array_length(?, 1) = 2", j.attempted_by) and
            p.uuid == fragment("uuid (?[2])", j.attempted_by)
      )
      |> where([_, p], is_nil(p.uuid))
      |> select([:id])

    query = join(Job, :inner, [j], x in subquery(subquery), on: j.id == x.id)

    {ta_count, nil} = transition_available(query, state)
    {td_count, nil} = transition_discarded(query, state)

    ta_count + td_count
  end

  defp transition_available(query, state) do
    Repo.update_all(
      state.conf,
      where(query, [j], j.attempt < j.max_attempts),
      set: [state: "available"]
    )
  end

  defp transition_discarded(query, state) do
    updates =
      if state.retry_exhausted do
        [set: [state: "available"], inc: [max_attempts: 1]]
      else
        [set: [state: "discarded", discarded_at: DateTime.utc_now()]]
      end

    Repo.update_all(state.conf, where(query, [j], j.attempt >= j.max_attempts), updates)
  end
end

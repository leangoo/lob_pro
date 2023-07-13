defmodule Oban.Pro.Plugins.DynamicLifeline do
  @moduledoc """
  The `DynamicLifeline` plugin uses producer records to periodically rescues orphaned jobs, i.e.
  jobs that are stuck in the `executing` state because the node was shut down before the job could
  finish.

  Without `DynamicLifeline` you'll need to manually rescue jobs stuck in the `executing` state.

  ## Using the Plugin

  To use the `DynamicLifeline` plugin, add the module to your list of Oban plugins in
  `config.exs`:

      config :my_app, Oban,
        plugins: [Oban.Pro.Plugins.DynamicLifeline]
        ...

  There isn't any configuration necessary. By default, the plugin rescues orphaned jobs every 1
  minute. If necessary, you can override the rescue interval:

      plugins: [{Oban.Pro.Plugins.DynamicLifeline, rescue_interval: :timer.minutes(5)}]

  If your system is under high load or produces a multitude of orphans you may wish to increase
  the query timeout beyond the `30s` default:

      plugins: [{Oban.Pro.Plugins.DynamicLifeline, timeout: :timer.minutes(1)}]

  Note that rescuing orphans relies on producer records as used by the `SmartEngine`.

  ## Rescuing Exhausted Jobs

  When a job's `attempt` matches its `max_attempts` its retries are considered "exhausted".
  Normally, the `DynamicLifeline` plugin transitions exhausted jobs to the `discarded` state and
  they won't be retried again. It does this for a couple of reasons:

  1. To ensure at-most-once semantics. Suppose a long-running job interacted with a non-idempotent
     service and was shut down while waiting for a reply; you may not want that job to retry.
  2. To prevent infinitely crashing BEAM nodes. Poorly behaving jobs may crash the node (through
     NIFs, memory exhaustion, etc.) We don't want to repeatedly rescue and rerun a job that
     repeatedly crashes the entire node.

  Discarding exhausted jobs may not always be desired. Use the `retry_exhausted` option if you'd
  prefer to retry exhausted jobs when they are rescued, rather than discarding them:

      plugins: [{Oban.Pro.Plugins.DynamicLifeline, retry_exhausted: true}]

  During rescues, with `retry_exhausted: true`, a job's `max_attempts` is incremented and it is
  moved back to the `available` state.

  ## Instrumenting with Telemetry

  The `DynamicLifeline` plugin adds the following metadata to the `[:oban,
  :plugin, :stop]` event:

  * `:rescued_jobs` — a list of jobs transitioned back to `available`

  * `:discarded_jobs` — a list of jobs transitioned to `discarded`

  _Note: jobs only include `id`, `queue`, and `state` fields._
  """

  @behaviour Oban.Plugin

  use GenServer

  import Ecto.Query, only: [join: 5, select: 3, where: 3]

  alias Oban.Pro.Producer
  alias Oban.Pro.Queue.SmartEngine
  alias Oban.{Job, Peer, Repo, Validation}

  @type option ::
          {:conf, Oban.Config.t()}
          | {:name, Oban.name()}
          | {:retry_exhausted, boolean()}
          | {:rescue_interval, timeout()}
          | {:timeout, timeout()}

  @base_meta %{rescued_count: 0, discarded_count: 0, rescued_jobs: [], discarded_jobs: []}

  defmodule State do
    @moduledoc false

    defstruct [
      :conf,
      :name,
      :rescue_timer,
      retry_exhausted: false,
      rescue_interval: :timer.minutes(1),
      timeout: :timer.seconds(30)
    ]
  end

  @doc false
  def child_spec(args), do: super(args)

  @impl Oban.Plugin
  @spec start_link([option()]) :: GenServer.on_start()
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
      {:timeout, timeout} -> Validation.validate_timeout(:timeout, timeout)
      option -> {:unknown, option, State}
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
      meta =
        state
        |> rescue_orphaned()
        |> Map.merge(meta)

      {:ok, meta}
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
    if Peer.leader?(state.conf) do
      subquery =
        Job
        |> where([j], not is_nil(j.queue) and j.state == "executing")
        |> join(:left, [j], p in Producer,
          on:
            fragment("array_length(?, 1) <> 1", j.attempted_by) and
              p.uuid == fragment("uuid (?[2])", j.attempted_by)
        )
        |> where([_, p], is_nil(p.uuid))

      query =
        Job
        |> join(:inner, [j], x in subquery(subquery), on: j.id == x.id)
        |> select([_, x], map(x, [:id, :queue, :state]))

      {res_count, res_jobs} = transition_available(query, state)
      {dis_count, dis_jobs} = transition_discarded(query, state)

      if state.retry_exhausted do
        %{@base_meta | rescued_count: res_count + dis_count, rescued_jobs: res_jobs ++ dis_jobs}
      else
        %{
          rescued_count: res_count,
          discarded_count: dis_count,
          rescued_jobs: res_jobs,
          discarded_jobs: dis_jobs
        }
      end
    else
      @base_meta
    end
  end

  defp transition_available(query, state) do
    Repo.update_all(
      state.conf,
      where(query, [j], j.attempt < j.max_attempts),
      [set: [state: "available"]],
      timeout: state.timeout
    )
  end

  defp transition_discarded(query, state) do
    updates =
      if state.retry_exhausted do
        [set: [state: "available"], inc: [max_attempts: 1]]
      else
        [set: [state: "discarded", discarded_at: DateTime.utc_now()]]
      end

    Repo.update_all(
      state.conf,
      where(query, [j], j.attempt >= j.max_attempts),
      updates,
      timeout: state.timeout
    )
  end
end

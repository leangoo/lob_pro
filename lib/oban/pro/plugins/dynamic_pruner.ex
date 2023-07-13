defmodule Oban.Pro.Plugins.DynamicPruner do
  @moduledoc """
  DynamicPruner enhances the default Pruner plugin's behaviour by allowing you to customize how
  jobs are retained in the jobs table. Where the Pruner operates on a fixed schedule and treats
  all jobs the same, with the DynamicPruner, you can use a flexible CRON schedule and provide
  custom rules for specific queues, workers, and job states.

  ## Using the Plugin

  To start using the `DynamicPruner` add the module to your list of Oban plugins
  in `config.exs`:

  ```elixir
  config :my_app, Oban,
    plugins: [Oban.Pro.Plugins.DynamicPruner]
    ...
  ```

  Without any additional options the pruner operates in maximum length mode (`max_len`) and
  retains a conservative 1,000 `completed`, `cancelled`, or `discarded` jobs. To increase the
  number of jobs retained you can provide your own `mode` configuration:

  ```elixir
  plugins: [{Oban.Pro.Plugins.DynamicPruner, mode: {:max_len, 50_000}}]
  ```

  Now the pruner will retain the most recent 50,000 jobs instead.

  A fixed limit on the number of jobs isn't always ideal. Often you want to retain jobs based on
  their age instead. For example, if you your application needs to ensure that a duplicate job
  hasn't been enqueued within the past 24 hours you need to retain jobs for at least 24 hours; a
  fixed limit simply won't work. For that we can use maximum age (`max_age`) mode instead:

  ```elixir
  plugins: [{Oban.Pro.Plugins.DynamicPruner, mode: {:max_age, 60 * 60 * 48}}]
  ```

  Here we've specified `max_age` using seconds, where `60 * 60 * 48` is the number of seconds in
  two days.

  Calculating the number of seconds in a period isn't especially readable, particularly when you
  have numerous `max_age` declarations in overrides (see below). For clarity you can specify the
  age's time unit as `:second`, `:minute`, `:hour`, `:day` or `:month`. Here is the same 48 hour
  configuration from above, but specified in terms of days:

  ```elixir
  plugins: [{Oban.Pro.Plugins.DynamicPruner, mode: {:max_age, {2, :days}}}]
  ```

  Now you can tell exactly how long jobs should be retained, without reverse calculating how many
  seconds an expression represents.

  ## Providing Overrides

  The `mode` option is indiscriminate when determining which jobs to prune. It pays no attention
  to which queue they are in, what worker the job is for, or which state they landed in. The
  `DynamicPruner` allows you to specify per-queue, per-worker and per-state overrides that fine
  tune pruning.

  We'll start with a simple example of limiting the total number of retained jobs in the `events`
  queue:

  ```elixir
  plugins: [{
    Oban.Pro.Plugins.DynamicPruner,
    mode: {:max_age, {7, :days}},
    queue_overrides: [events: {:max_len, 1_000}]
  }]
  ```

  With this configuration most jobs will be retained for seven days, but we'll only keep the
  latest 1,000 jobs in the events queue. We can extend this further and override all of our queues
  (and omit the default mode entirely):

  ```elixir
  plugins: [{
    Oban.Pro.Plugins.DynamicPruner,
    queue_overrides: [
      default: {:max_age, {6, :hours}},
      analysis: {:max_age, {1, :day}},
      events: {:max_age, {10, :minutes}},
      mailers: {:max_age, {2, :weeks}},
      media: {:max_age, {2, :months}}
    ]
  }]
  ```

  When pruning by queue isn't granular enough you can provide overrides by worker instead:

  ```elixir
  plugins: [{
    Oban.Pro.Plugins.DynamicPruner,
    worker_overrides: [
      "MyApp.BusyWorker": {:max_age, {1, :day}},
      "MyApp.SecretWorker": {:max_age, {1, :second}},
      "MyApp.HistoricWorker": {:max_age, {1, :month}}
    ]
  }]
  ```

  You can also override by state, which allows you to keep discarded jobs for inspection while
  quickly purging cancelled or successfully completed jobs:

  ```elixir
  plugins: [{
    Oban.Pro.Plugins.DynamicPruner,
    state_overrides: [
      cancelled: {:max_age, {1, :hour}},
      completed: {:max_age, {1, :day}},
      discarded: {:max_age, {1, :month}}
    ]
  }]
  ```

  Naturally you can mix and match overrides to finely control job retention:

  ```elixir
  plugins: [{
    Oban.Pro.Plugins.DynamicPruner,
    mode: {:max_age, {7, :days}},
    queue_overrides: [events: {:max_age, {10, :minutes}}],
    worker_overrides: ["MyApp.SecretWorker": {:max_age, {1, :second}}],
    state_overrides: [discarded: {:max_age, {2, :days}}]
  }]
  ```

  ### Override Precedence

  Overrides are applied sequentially, in this order:

  1. Queue
  2. State
  3. Worker
  4. Default

  Using the example above, jobs in the `events` queue are deleted first, followed by jobs in the
  `discarded` state, then the `MyApp.SecretWorker` worker, and finally any other jobs older than 7
  days that **weren't covered by any overrides**.

  ### Preventing Timeouts with Overrides

  Worker override queries aren't able to use any of Oban's standard indexes. If you're processing
  a high volume of jobs, pruning with worker overrides may be extremely slow due to sequential
  scans. To prevent timeouts, and speed up pruning altogether, you should add a compound index to
  the `oban_jobs` table:

  ```elixir
  create_if_not_exists index(:oban_jobs, [:worker, :state, :id], concurrently: true)
  ```

  ## Retaining Jobs Forever

  To retain jobs in a queue, state, or for a particular worker forever (without using something
  like `{:max_age, {999, :years}}` use `:infinity` as the length or duration:

  ```elixir
  plugins: [{
    Oban.Pro.Plugins.DynamicPruner,
    mode: {:max_age, {7, :days}},
    state_overrides: [
      cancelled: {:max_len, :infinity},
      discarded: {:max_age, :infinity}
    ]
  }]
  ```

  ## Keeping Up With Inserts

  With the default settings the `DynamicPruner` will only delete 10,000 jobs each time it prunes.
  The limit exists to prevent connection timeouts and excessive table locks. A busy system can
  easily insert more than 10,000 jobs per minute during standard operation. If you find that jobs
  are accumulating despite active pruning you can override the `limit`.

  Here we set the delete limit to 25,000 and give it 60 seconds to complete:

  ```elixir
  plugins: [{
    Oban.Pro.Plugins.DynamicPruner,
    mode: {:max_len, 100_000},
    limit: 25_000,
    timeout: :timer.seconds(60)
  }]
  ```

  Deleting in PostgreSQL is _very_ fast, and the 10k default is rather conservative. Feel free to
  increase the limit to a number that your system can handle.

  ## Setting a Schedule

  By default, pruning happens at the top of every minute based on the CRON schedule `* * * * *`.
  You're free to set any CRON schedule you prefer for greater control over when to prune. For
  example, to prune once an hour instead:

  ```elixir
  plugins: [{Oban.Pro.Plugins.DynamicPruner, schedule: "0 * * * *"}]
  ```

  Or, to prune once a day at midnight in your local timezone:

  ```elixir
  plugins: [{
    Oban.Pro.Plugins.DynamicPruner,
    limit: 100_000,
    schedule: "0 0 * * *",
    timezone: "America/Chicago",
    timeout: :timer.secons(60)
  }]
  ```

  Pruning less frequently can reduce load on your system, particularly if you're using
  multiple [overrides](#providing-overrides). However, be sure to set a higher `limit` and
  `timeout` (as shown above) to compensate for more accumulated jobs.

  ## Executing a Callback Before Delete

  Sometimes jobs are a historic record of activity and it's desirable to operate on them _before_
  they're deleted. For example, you may want copy some jobs into cold storage prior to completion.

  To accomplish this, specify a callback to execute before proceeding with the deletion.:

  ```elixir
  defmodule DeleteHandler do
    def call(job_ids) do
      # Use the ids at this point, from within a transaction
    end
  end

  plugins: [{
    Oban.Pro.Plugins.DynamicPruner,
    mode: {:max_age, {7, :days}},
    before_delete: {DeleteHandler, :call, []}
  }]
  ```

  The callback receives a list of the ids for the jobs that are about to be deleted. The callback
  runs within the same transaction that's used for deletion, and you should keep it quick or move
  heavy processing to an async process. Note that because it runs in the same transaction as
  deletion, the jobs _won't be available after the callback exits_.

  To pass in extra arguments as "configuration" you can provide args to the callback MFA:

  ```elixir
  defmodule DeleteHandler do
    import Ecto.Query

    def call(job_ids, storage_name) do
      jobs = MyApp.Repo.all(where(Oban.Job, [j], j.id in ^job_ids))

      Storage.call(storage_name, jobs)
    end
  end

  before_delete: {DeleteHandler, :call, [ColdStorage]}
  ```

  ## Implementation Notes

  Some additional notes about pruning in general and nuances of the `DynamicPruner` plugin:

  * Pruning is best-effort and performed out-of-band. This means that all limits are soft; jobs
    beyond a specified age may not be pruned immediately after jobs complete.

  * Pruning is only applied to jobs that are `completed`, `cancelled` or `discarded` (has reached
    the maximum number of retries or has been manually killed). It'll never delete a new,
    scheduled, or retryable job.

  * Only a single node will prune at any given time, which prevents potential deadlocks between
    transactions.

  ## Instrumenting with Telemetry

  The `DynamicPruner` plugin adds the following metadata to the `[:oban, :plugin, :stop]` event:

  * `:pruned_jobs` - the jobs that were deleted from the database

  _Note: jobs only include `id`, `queue`, and `state` fields._
  """

  @behaviour Oban.Plugin

  use GenServer

  import Ecto.Query

  alias Ecto.Multi
  alias Oban.Cron.Expression
  alias Oban.Plugins.Cron
  alias Oban.{Config, Job, Peer, Repo, Validation}

  @type time_unit ::
          :second
          | :seconds
          | :minute
          | :minutes
          | :hour
          | :hours
          | :day
          | :days
          | :week
          | :weeks
          | :month
          | :month

  @type max_age :: pos_integer() | {pos_integer(), time_unit()}

  @type max_len :: pos_integer()

  @type mode :: {:max_age, max_age()} | {:max_len, max_len()}

  @type state_override :: {:completed | :cancelled | :discarded, mode()}

  @type queue_override :: {atom(), mode()}

  @type worker_override :: {module(), mode()}

  @type override :: state_override() | queue_override() | worker_override()

  @type option ::
          {:conf, Oban.Config.t()}
          | {:after, timeout()}
          | {:interval, pos_integer()}
          | {:name, Oban.name()}

  @modes [:max_age, :max_len]
  @states [:completed, :cancelled, :discarded]
  @time_units [
    :second,
    :seconds,
    :minute,
    :minutes,
    :hour,
    :hours,
    :day,
    :days,
    :week,
    :weeks,
    :month,
    :months
  ]

  defmodule State do
    @moduledoc false

    defstruct [
      :before_delete,
      :conf,
      :name,
      :schedule,
      :timer,
      mode: {:max_len, 1_000},
      limit: 10_000,
      timeout: :timer.seconds(60),
      timezone: "Etc/UTC",
      queue_overrides: [],
      state_overrides: [],
      worker_overrides: []
    ]
  end

  @doc false
  def child_spec(args), do: super(args)

  @impl Oban.Plugin
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  @impl Oban.Plugin
  def validate(opts) do
    Validation.validate(opts, fn
      {:conf, _} -> :ok
      {:name, _} -> :ok
      {:before_delete, mfa} -> validate_before_delete(mfa)
      {:limit, limit} -> Validation.validate_integer(:limit, limit)
      {:mode, mode} -> validate_mode(mode)
      {:queue_overrides, overrides} -> validate_overrides(:queue_overrides, overrides)
      {:schedule, schedule} -> validate_schedule(schedule)
      {:state_overrides, overrides} -> validate_overrides(:state_overrides, overrides)
      {:timeout, timeout} -> Validation.validate_integer(:timeout, timeout)
      {:timezone, timezone} -> Validation.validate_timezone(:timezone, timezone)
      {:worker_overrides, overrides} -> validate_overrides(:worker_overrides, overrides)
      option -> {:unknown, option, State}
    end)
  end

  # Callbacks

  @impl GenServer
  def init(opts) do
    Validation.validate!(opts, &validate/1)

    opts =
      opts
      |> Keyword.put_new(:schedule, "* * * * *")
      |> Keyword.update!(:schedule, &Expression.parse!/1)

    Process.flag(:trap_exit, true)

    state =
      State
      |> struct!(opts)
      |> schedule_prune()

    :telemetry.execute([:oban, :plugin, :init], %{}, %{conf: state.conf, plugin: __MODULE__})

    {:ok, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    if is_reference(state.timer), do: Process.cancel_timer(state.timer)

    :ok
  end

  @impl GenServer
  def handle_info(:prune, state) do
    meta = %{conf: state.conf, plugin: __MODULE__}

    :telemetry.span([:oban, :plugin], meta, fn ->
      with {:ok, extra} <- prune_jobs(state), do: {:ok, Map.merge(meta, extra)}
    end)

    {:noreply, schedule_prune(state)}
  end

  # Scheduling

  defp schedule_prune(state) do
    timer = Process.send_after(self(), :prune, Cron.interval_to_next_minute())

    %{state | timer: timer}
  end

  # Query

  defp prune_jobs(state) do
    {:ok, datetime} = DateTime.now(state.timezone)

    if Peer.leader?(state.conf) and Expression.now?(state.schedule, datetime) do
      queue_pruned =
        for {queue, mode} <- state.queue_overrides do
          Job
          |> query_for_queues(:any, [to_string(queue)])
          |> delete_for_mode(mode, state)
        end

      state_pruned =
        for {queue_state, mode} <- state.state_overrides do
          Job
          |> query_for_states(:any, [to_string(queue_state)])
          |> delete_for_mode(mode, state)
        end

      worker_pruned =
        for {worker, mode} <- state.worker_overrides do
          Job
          |> query_for_workers(:any, [to_string(worker)])
          |> delete_for_mode(mode, state)
        end

      default_pruned =
        Job
        |> query_for_queues(:not, string_keys(state.queue_overrides))
        |> query_for_states(:not, string_keys(state.state_overrides))
        |> query_for_workers(:not, string_keys(state.worker_overrides))
        |> delete_for_mode(state.mode, state)

      pruned = List.flatten([queue_pruned, state_pruned, worker_pruned, default_pruned])

      {:ok, %{pruned_count: length(pruned), pruned_jobs: pruned}}
    else
      {:ok, %{pruned_count: 0, pruned_jobs: []}}
    end
  end

  defp string_keys(keyword) do
    for {key, _} <- keyword, do: to_string(key)
  end

  defp query_for_queues(query, :not, []), do: query
  defp query_for_queues(query, :not, queues), do: where(query, [j], j.queue not in ^queues)
  defp query_for_queues(query, :any, queues), do: where(query, [j], j.queue in ^queues)

  defp query_for_states(query, :not, []), do: query
  defp query_for_states(query, :not, states), do: where(query, [j], j.state not in ^states)
  defp query_for_states(query, :any, states), do: where(query, [j], j.state in ^states)

  defp query_for_workers(query, :not, []), do: query
  defp query_for_workers(query, :not, workers), do: where(query, [j], j.worker not in ^workers)
  defp query_for_workers(query, :any, workers), do: where(query, [j], j.worker in ^workers)

  defp delete_for_mode(_query, {_mode, :infinity}, _state), do: []

  defp delete_for_mode(query, {:max_age, age}, state) do
    time = to_timestamp(age)

    query
    |> where([j], j.attempted_at < ^time or j.cancelled_at < ^time or j.discarded_at < ^time)
    |> select([j], %{id: j.id, queue: j.queue, state: j.state, worker: j.worker, rn: 100_000_000})
    |> delete_all(state.limit + 1, state)
  end

  defp delete_for_mode(query, {:max_len, len}, state) do
    query
    |> select([j], %{
      id: j.id,
      queue: j.queue,
      state: j.state,
      worker: j.worker,
      rn: fragment("row_number() over (order by id desc)")
    })
    |> delete_all(len, state)
  end

  defp delete_all(query, offset, state) do
    %Config{log: log, prefix: prefix} = state.conf

    subquery =
      query
      |> where([j], j.state in ["cancelled", "completed", "discarded"])
      |> order_by(asc: :id)
      |> limit(^state.limit)

    query =
      Job
      |> join(:inner, [j], x in subquery(subquery), on: j.id == x.id and x.rn > ^offset)
      |> select([_, x], map(x, [:id, :queue, :state]))

    multi =
      Multi.new()
      |> Multi.run(:callback, &apply_before_delete(&1, &2, query, state))
      |> Multi.delete_all(:deleted, query, log: log, prefix: prefix, timeout: state.timeout)

    {:ok, %{deleted: {_count, deleted}}} =
      Repo.transaction(state.conf, multi, timeout: state.timeout)

    deleted
  end

  defp apply_before_delete(_repo, _changes, query, state) do
    with {mod, fun, args} <- state.before_delete do
      ids =
        state.conf
        |> Repo.all(query)
        |> Enum.map(& &1.id)

      apply(mod, fun, [ids | args])
    end

    {:ok, []}
  end

  defp to_timestamp(seconds) when is_integer(seconds) do
    DateTime.add(DateTime.utc_now(), -seconds, :second)
  end

  defp to_timestamp({seconds, :second}), do: to_timestamp(seconds)
  defp to_timestamp({seconds, :seconds}), do: to_timestamp(seconds)
  defp to_timestamp({minutes, :minute}), do: to_timestamp(minutes * 60)
  defp to_timestamp({minutes, :minutes}), do: to_timestamp({minutes, :minute})
  defp to_timestamp({hours, :hour}), do: to_timestamp(hours * 60 * 60)
  defp to_timestamp({hours, :hours}), do: to_timestamp({hours, :hour})
  defp to_timestamp({days, :day}), do: to_timestamp(days * 24 * 60 * 60)
  defp to_timestamp({days, :days}), do: to_timestamp({days, :day})
  defp to_timestamp({weeks, :week}), do: to_timestamp(weeks * 7 * 24 * 60 * 60)
  defp to_timestamp({weeks, :weeks}), do: to_timestamp({weeks, :week})
  defp to_timestamp({months, :month}), do: to_timestamp(months * 30 * 24 * 60 * 60)
  defp to_timestamp({months, :months}), do: to_timestamp({months, :month})

  # Validations

  defp validate_before_delete({mod, fun, args})
       when is_atom(mod) and is_atom(fun) and is_list(args) do
    cond do
      not Code.ensure_loaded?(mod) ->
        {:error, "module #{inspect(mod)} can't be loaded"}

      not function_exported?(mod, fun, length(args) + 1) ->
        {:error, "no function #{inspect(fun)} with arity #{length(args) + 1}"}

      true ->
        :ok
    end
  end

  defp validate_before_delete(mfa) do
    {:error, "expected :before_delete to be a {module, fun, args} tuple, got: #{inspect(mfa)}"}
  end

  defp validate_mode({mode, :infinity}) when mode in @modes, do: :ok
  defp validate_mode({mode, int}) when is_integer(int) and mode in @modes, do: :ok

  defp validate_mode({mode, {value, time_unit}})
       when is_integer(value) and mode in @modes and time_unit in @time_units,
       do: :ok

  defp validate_mode(mode) do
    {:error, "expected :mode to be {:max_len, length} or {:max_age, age}, got: #{inspect(mode)}"}
  end

  defp validate_overrides(:state_overrides = parent_key, overrides) do
    Validation.validate(parent_key, overrides, fn
      {state, mode} when state in @states ->
        validate_mode(mode)

      {state, _mode} ->
        {:error, "expected state to be included in #{inspect(@states)}, got: #{inspect(state)}"}

      override ->
        {:error, "expected #{inspect(parent_key)} to contain a tuple, got: #{inspect(override)}"}
    end)
  end

  defp validate_overrides(parent_key, overrides) do
    Validation.validate(parent_key, overrides, fn
      {key, mode} when is_atom(key) ->
        validate_mode(mode)

      override ->
        {:error, "expected #{inspect(parent_key)} to contain a tuple, got: #{inspect(override)}"}
    end)
  end

  defp validate_schedule(schedule) do
    Expression.parse!(schedule)

    :ok
  rescue
    error in [ArgumentError] -> {:error, error}
  end
end

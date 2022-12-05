defmodule Oban.Pro.Testing do
  @moduledoc """
  Advanced helpers for testing supervised Oban instances, workers, and making assertions about
  enqueued jobs.

  The `Oban.Pro.Testing` module is a drop-in replacement for `Oban.Testing`, with additional
  functions tailored toward integration testing and Pro modules.

  ## Usage in Tests

  The most convenient way to use `Oban.Pro.Testing` is to `use` the module:

      use Oban.Pro.Testing, repo: MyApp.Repo

  Other repo-specific configuration options can also be used:

      use Oban.Pro.Testing, repo: MyApp.Repo, prefix: "private", log: :debug

  If you already have `use Oban.Testing` in your tests or test cases, simply replace it with `use
  Oban.Pro.Testing`.

  ## Naming Convention

  The testing helpers in this module adhere to the following naming convention:

  * `perform_*` — executes jobs locally, without touching the database, for unit testing.

  * `drain_*` — execute jobs inline, for integration testing.

  * `run_*` — insert jobs into the database and execute them inline, for integration testing.

  ## Shared Repo Options

  The `use` macro accepts all of these repo-specific configuration options, and they may be passed
  to all database functions (`run_`, `drain_`, etc.)

  * `:log` — a usable log level or `false` to disable logging. See `t:Logger.level/0` for valid
    options.

  * `:prefix` — an optional database prefix. Defaults to `public`.

  * `:repo` — the name of an Ecto repo, which should be running in sandbox mode.
  """

  import Ecto.Query, only: [where: 3]
  import ExUnit.Assertions, only: [assert: 2]
  import ExUnit.Callbacks, only: [on_exit: 2, start_supervised!: 1]

  alias Ecto.Adapters.SQL.Sandbox
  alias Ecto.Changeset
  alias Oban.{Config, Job, Repo, Worker}
  alias Oban.Pro.Queue.SmartEngine
  alias Oban.Pro.Workers.{Chunk, Workflow}
  alias Oban.Queue.Executor

  @type repo_option :: {:log, false | Logger.level()} | {:prefix, String.t()} | {:repo, module()}

  @type drain_option ::
          repo_option()
          | {:queue, atom()}
          | {:with_limit, pos_integer()}
          | {:with_recursion, boolean()}
          | {:with_safety, boolean()}
          | {:with_scheduled, boolean()}
          | {:with_summary, boolean()}

  @type drain_summary :: %{
          cancelled: non_neg_integer(),
          completed: non_neg_integer(),
          discarded: non_neg_integer(),
          exhausted: non_neg_integer(),
          retryable: non_neg_integer(),
          scheduled: non_neg_integer()
        }

  @type drain_result :: drain_summary() | [Job.t()]

  @type perform_option :: Job.option() | repo_option()

  @typedoc """
  Batch callback identifiers, correlating to a `handle_` callback function.
  """
  @type callback :: :attempted | :completed | :discarded | :exhausted

  defmodule Peer do
    @moduledoc false

    def start_link(opts), do: Agent.start_link(fn -> true end, name: opts[:name])

    def leader?(pid, _timeout), do: Agent.get(pid, & &1)
  end

  @doc false
  defmacro __using__(repo_opts) do
    # NOTE: This is an ideal spot to use Keyword.validate! once we require Elixir 1.13+
    _repo = Keyword.fetch!(repo_opts, :repo)

    quote do
      def all_enqueued(opts \\ []) do
        unquote(repo_opts)
        |> Keyword.merge(opts)
        |> Oban.Pro.Testing.all_enqueued()
      end

      def assert_enqueued(opts \\ [], timeout \\ :none) do
        unquote(repo_opts)
        |> Keyword.merge(opts)
        |> Oban.Pro.Testing.assert_enqueued(timeout)
      end

      def refute_enqueued(opts \\ [], timeout \\ :none) do
        unquote(repo_opts)
        |> Keyword.merge(opts)
        |> Oban.Pro.Testing.refute_enqueued(timeout)
      end

      def drain_jobs(opts \\ []) do
        unquote(repo_opts)
        |> Keyword.merge(opts)
        |> Oban.Pro.Testing.drain_jobs()
      end

      def perform_job(worker, args, opts \\ []) do
        Oban.Pro.Testing.perform_job(worker, args, Keyword.merge(unquote(repo_opts), opts))
      end

      def process_job(worker, args, opts \\ []) do
        Oban.Pro.Testing.perform_job(worker, args, Keyword.merge(unquote(repo_opts), opts))
      end

      def perform_callback(worker, callback, args, opts \\ []) do
        opts = Keyword.merge(unquote(repo_opts), opts)

        Oban.Pro.Testing.perform_callback(worker, callback, args, opts)
      end

      def perform_chunk(worker, args, opts \\ []) do
        Oban.Pro.Testing.perform_chunk(worker, args, Keyword.merge(unquote(repo_opts), opts))
      end

      def run_batch([_ | _] = batch, opts \\ []) do
        Oban.Pro.Testing.run_batch(batch, Keyword.merge(unquote(repo_opts), opts))
      end

      def run_chunk([_ | _] = chunk, opts \\ []) do
        Oban.Pro.Testing.run_chunk(chunk, Keyword.merge(unquote(repo_opts), opts))
      end

      def run_jobs([_ | _] = changesets, opts \\ []) do
        Oban.Pro.Testing.run_jobs(changesets, Keyword.merge(unquote(repo_opts), opts))
      end

      def run_workflow(%_{} = workflow, opts \\ []) do
        Oban.Pro.Testing.run_workflow(workflow, Keyword.merge(unquote(repo_opts), opts))
      end

      def start_supervised_oban!(opts \\ []) do
        unquote(repo_opts)
        |> Keyword.merge(opts)
        |> Oban.Pro.Testing.start_supervised_oban!()
      end
    end
  end

  @callbacks ~w(attempted completed discarded exhausted)a

  @default_supervised_opts [
    engine: Oban.Pro.Queue.SmartEngine,
    notifier: Oban.Notifiers.PG,
    peer: Oban.Pro.Testing.Peer,
    poll_interval: :infinity,
    shutdown_grace_period: 250
  ]

  @doc """
  Retrieve all currently enqueued jobs matching a set of criteria.

  This is a wrapper around `Oban.Testing.all_enqueued/2`, see `Oban.Testing` for more details.

  ## Options

  See [shared options](#module-shared-repo-options) for additional repo-specific options.
  """
  @doc since: "0.11.0"
  @spec all_enqueued(Keyword.t()) :: [Job.t()]
  def all_enqueued(opts) do
    {repo, opts} = Keyword.pop!(opts, :repo)

    Oban.Testing.all_enqueued(repo, opts)
  end

  @doc """
  Assert that a job with particular criteria is enqueued.

  This is a wrapper around `Oban.Testing.assert_enqueued/2`, see `Oban.Testing` for more details.

  ## Options

  See [shared options](#module-shared-repo-options) for additional repo-specific options.
  """
  @doc since: "0.11.0"
  @spec assert_enqueued(Keyword.t(), timeout() | :none) :: true
  def assert_enqueued(opts, timeout \\ :none) do
    {repo, opts} = Keyword.pop!(opts, :repo)

    if timeout == :none do
      Oban.Testing.assert_enqueued(repo, opts)
    else
      Oban.Testing.assert_enqueued(repo, opts, timeout)
    end
  end

  @doc """
  Refute that a job with particular criteria is enqueued.

  This is a wrapper around `Oban.Testing.refute_enqueued/2`, see `Oban.Testing` for more details.

  ## Options

  See [shared options](#module-shared-repo-options) for additional repo-specific options.
  """
  @doc since: "0.11.0"
  @spec refute_enqueued(Keyword.t(), timeout() | :none) :: false
  def refute_enqueued(opts, timeout \\ :none) do
    {repo, opts} = Keyword.pop!(opts, :repo)

    if timeout == :none do
      Oban.Testing.refute_enqueued(repo, opts)
    else
      Oban.Testing.refute_enqueued(repo, opts, timeout)
    end
  end

  @doc """
  Synchronously execute jobs in one or all queues, from within the current process.

  Jobs that are enqueued by a process when `Ecto` is in sandbox mode are only visible to that
  process. Calling `drain_jobs/1` allows you to control when the jobs are executed and to wait
  synchronously for all jobs to complete.

  This function provides several distinct advantages over the standard `Oban.drain_queue/2`:

    * It can drain jobs across one or all queues simultaneously
    * It can return the drained jobs rather than a count summary
    * It optimizes the defaults for testing batches, workflows, etc.
    * It always uses the `SmartEngine` to guarantee that Pro worker features work as expected

  ## Options

  * `:queue` - an atom specifying the queue to drain, or `:all` to drain jobs across all queues at
    once. Defaults to `:all` when no queue is provided.

  * `:with_limit` — the maximum number of jobs to fetch for draining at once. The limit only
    impacts how many jobs are fetched at once, _not_ concurrency. When recursion is enabled this
    is how many jobs are processed per-iteration, and it defaults to `1`. Otherwise, there isn't a
    limit and all available jobs are fetched.

  * `:with_recursion` — whether to draining jobs recursively, or all in a single pass. Either way,
    jobs are processed sequentially, one at a time. Recursion is required when jobs insert other
    jobs (e.g. batches), or depend on the execution of other jobs (e.g. workflows). Defaults to
    `true`.

  * `:with_safety` — whether to silently catch errors when draining. When `false`, raised
    exceptions or unhandled exits are reraised (unhandled exits are wrapped in `Oban.CrashError`).
    Defaults to `false`.

  * `:with_scheduled` — whether to include scheduled or retryable jobs when draining. In recursive
    mode, which is the default, this will include snoozed jobs, and may lead to an infinite loop if
    the job snoozes repeatedly. Defaults to `true`.

  * `:with_summary` — whether to summarize execution results with a map of counts by state, or
    return a list of each job that was drained. Defaults to `true`, which returns a summary map.

  See [shared options](#module-shared-repo-options) for additional repo-specific options.

  ## Examples

  Drain all available jobs across all queues:

      assert %{completed: 3} = drain_jobs(queue: :all)

  Drain and return all executed jobs without a count summary:

      assert [%Oban.Job{}, %Oban.Job{}] = drain_jobs(with_summary: false)

  Drain including a job that you expect to raise:

      assert_raise RuntimeError, fn -> drain_jobs(queue: :risky) end

  Drain without recursion to identify snoozed jobs:

      assert %{scheduled: 3} = drain_jobs(with_recursion: false)

  Drain without staging scheduled jobs:

      assert %{completed: 1, scheduled: 0} = drain_jobs(with_scheduled: false)

  Drain within a custom prefix:

      assert %{completed: 3} = drain_jobs(queue: :default, prefix: "private")

  Drain using a specific repo (necessary when calling this function directly):

      assert %{completed: 3} = drain_jobs(queue: :default, repo: MyApp.Repo)
  """
  @doc since: "0.11.0"
  @spec drain_jobs([drain_option()]) :: drain_result()
  def drain_jobs(opts) when is_list(opts) do
    {conf_opts, opts} = Keyword.split(opts, [:log, :prefix, :repo])

    conf =
      @default_supervised_opts
      |> Keyword.merge(conf_opts)
      |> Config.new()

    with_limit = if opts[:with_recursion], do: 1, else: 999_999_999

    opts =
      opts
      |> Map.new()
      |> Map.put_new(:queue, :all)
      |> Map.update!(:queue, &if(&1 == :all, do: :__all__, else: &1))
      |> Map.put_new(:with_limit, with_limit)
      |> Map.put_new(:with_recursion, true)
      |> Map.put_new(:with_safety, false)
      |> Map.put_new(:with_scheduled, true)
      |> Map.put_new(:with_summary, true)

    drain(conf, [], opts)
  end

  @doc since: "0.11.0"
  @spec perform_job(Worker.t(), term(), [perform_option()]) :: Worker.result()
  defdelegate perform_job(worker, args, opts), to: Oban.Testing

  @doc since: "0.9.0"
  @doc deprecated: "Use perform_job/3 instead"
  @spec process_job(Worker.t(), term(), [perform_option()]) :: Worker.result()
  defdelegate process_job(worker, args, opts), to: Oban.Testing, as: :perform_job

  @doc """
  Construct and execute a job with a batch `handle_*` callback.

  This helper verifies that the batch worker exports the requested callback handler,
  along with the standard assertions made by `perform_job/3`.

  ## Examples

  Execute the `handle_attempted` callback without any args:

      assert :ok = perform_callback(MyBatch, :attempted, %{})

  Execute the `handle_exhausted` callback with args:

      assert :ok = perform_callback(MyBatch, :exhausted, %{for_account_id: 123})
  """
  @doc since: "0.11.0"
  @spec perform_callback(Worker.t(), callback(), term(), [perform_option()]) :: Worker.result()
  def perform_callback(worker, callback, args, opts) when is_list(opts) do
    assert_valid_callback(worker, callback)

    opts =
      opts
      |> Keyword.put_new(:meta, %{})
      |> Keyword.update!(:meta, &Map.put_new(&1, "callback", to_string(callback)))
      |> Keyword.update!(:meta, &Map.put_new(&1, "batch_id", worker.gen_id()))

    perform_job(worker, args, opts)
  end

  @doc """
  Construct a list of jobs and process them with a Chunk worker.

  Like `perform_job/3`, this helper reduces boilerplate when constructing jobs and checks for
  common pitfalls. Unlike `perform_job/3`, this helper calls the chunk's `process/1` function
  directly and it **won't trigger telemetry events**.

  ## Examples

  Successfully process a chunk of jobs:

      assert :ok = perform_chunk(MyChunk, [%{id: 1}, %{id: 2}])

  Process a chunk of jobs with job options:

      assert :ok = perform_chunk(MyChunk, [%{id: 1}, %{id: 2}], attempt: 5, priority: 3)
  """
  @doc since: "0.11.0"
  @spec perform_chunk(Worker.t(), [term()], [perform_option()]) :: Chunk.result()
  def perform_chunk(worker, args_list, opts) when is_list(args_list) and is_list(opts) do
    {conf_opts, opts} = Keyword.split(opts, [:log, :prefix, :repo])

    opts = Keyword.put_new(opts, :attempt, 1)
    conf = Config.new(conf_opts)

    result =
      args_list
      |> Enum.map(fn args ->
        now = DateTime.utc_now()

        args
        |> worker.new(opts)
        |> Changeset.update_change(:args, &json_encode_decode/1)
        |> Changeset.put_change(:attempted_at, now)
        |> Changeset.put_change(:scheduled_at, now)
        |> Changeset.apply_action!(:insert)
        |> Map.replace!(:conf, conf)
      end)
      |> worker.process()

    assert_valid_chunk_result(result)

    result
  end

  @doc """
  Insert and execute a complete batch of jobs, along with callbacks, within the test process.

  ## Options

  Accepts all options for `drain_jobs/1`, including the repo-specific options listed in
  [shared-options](#module-shared-repo-options).

  ## Examples

  Run a batch:

       ids
       |> Enum.map(&MyBatch.new(%{id: &1}))
       |> MyBatch.new_batch()
       |> run_batch()

  Run a batch with a specific repo (necessary when calling this function directly):

      run_batch(my_batch, repo: MyApp.Repo, prefix: "private")
  """
  @doc since: "0.11.0"
  @spec run_batch([Job.changeset()], [drain_option()]) :: drain_result()
  def run_batch([_ | _] = batch, opts) when is_list(opts) do
    run_jobs(batch, opts)
  end

  @doc """
  Insert and execute a chunked jobs within the test process.

  This helper overrides the chunk's `timeout` to force immediate processing of jobs up to the
  chunk size.

  ## Options

  Accepts all options for `drain_jobs/1`, including the repo-specific options listed in
  [shared-options](#module-shared-repo-options).

  ## Examples

  Run jobs in chunks:

      1..50
      |> Enum.map(&MyChunk.new(%{id: &1}))
      |> run_chunk()

  Run chunks with an explicit repo (necessary when calling this function directly):

      run_chunk(changesets, repo: MyApp.Repo, prefix: "private")

  Run chunks only for a specific queue:

      run_chunk(changesets, queue: "default")
  """
  @doc since: "0.11.0"
  @spec run_chunk([Job.changeset()], [drain_option()]) :: drain_result()
  def run_chunk([_ | _] = chunk, opts) when is_list(opts) do
    changesets =
      Enum.map(chunk, fn changeset ->
        Changeset.update_change(changeset, :meta, &Map.put(&1, :chunk_timeout, 0))
      end)

    run_jobs(changesets, Keyword.put_new(opts, :with_limit, 1))
  end

  @doc """
  Insert and execute jobs synchronously, within the test process.

  This is the basis of all other `run_*` helpers.

  ## Options

  Accepts all options for `drain_jobs/1`, including the repo-specific options listed in
  [shared-options](#module-shared-repo-options).

  ## Examples

  Run a list of jobs:

      ids
      |> Enum.map(&MyWorker.new(%{id: &1}))
      |> run_jobs()

  Run jobs with an explicit repo (necessary when calling this function directly):

      run_jobs(changesets, repo: MyApp.Repo)
  """
  @doc since: "0.11.0"
  @spec run_jobs([Job.changeset()], [drain_option()]) :: drain_result()
  def run_jobs([_ | _] = changesets, opts) when is_list(changesets) and is_list(opts) do
    conf_opts = Keyword.take(opts, [:log, :prefix, :repo])

    @default_supervised_opts
    |> Keyword.merge(conf_opts)
    |> Config.new()
    |> SmartEngine.insert_all_jobs(changesets, [])

    opts
    |> Keyword.put_new(:queue, :all)
    |> drain_jobs()
  end

  @doc """
  Insert and execute a workflow synchronously, within the test process.

  This helper augments the workflow with options optimized for testing, but it will still respect
  all standard workflow options.

  ## Options

  Accepts all options for `drain_jobs/1`, including the repo-specific options listed in
  [shared-options](#module-shared-repo-options).

  ## Examples

  Run a basic workflow:

      MyFlow.new_workflow()
      |> MyFlow.add(:a, MyFlow.new(%{id: 1}))
      |> MyFlow.add(:b, MyFlow.new(%{id: 2}), deps: [:a])
      |> MyFlow.add(:c, MyFlow.new(%{id: 3}), deps: [:b])
      |> run_workflow()

  Run a workflow and match on returned jobs, but be careful that the execution
  order may differ from the insertion order:

      workflow =
        MyFlow.new_workflow()
        |> MyFlow.add(:a, MyFlow.new(%{id: 1}))
        |> MyFlow.add(:b, MyFlow.new(%{id: 2}), deps: [:a])
        |> MyFlow.add(:c, MyFlow.new(%{id: 3}), deps: [:b])

      [_job_a, _job_b, _job_c] = run_workflow(workflow, with_summary: false)

  Run a workflow with an explicit repo (necessary when calling this function directly):

      run_workflow(workflow, repo: MyApp.Repo)
  """
  @doc since: "0.11.0"
  @spec run_workflow(Workflow.t(), [Workflow.new_option() | drain_option()]) :: drain_result()
  def run_workflow(%Workflow{changesets: changesets}, opts \\ []) when is_list(opts) do
    work_opts =
      opts
      |> Keyword.take([:waiting_limit, :waiting_delay, :waiting_snooze])
      |> Map.new()
      |> Map.put_new(:waiting_limit, 1)
      |> Map.put_new(:waiting_delay, 1)
      |> Map.put_new(:waiting_snooze, 1)

    changesets =
      Enum.map(changesets, fn changeset ->
        Changeset.update_change(changeset, :meta, &Map.merge(&1, work_opts))
      end)

    run_jobs(changesets, opts)
  end

  @doc """
  Start a supervised Oban instance under the test supervisor.

  All valid Oban options are accepted. The supervised instance is registered with a unique
  reference, rather than the default `Oban`. That prevents any conflict with Oban instances
  started by your Application, or with other tests running asynchronously.

  Furthermore, this helper automatically adds sandbox allowances for any plugins or queue
  producers, allowing tests to run async.

  After the test finishes the test process will wait for the Oban instance to shut down cleanly.

  ## Running Jobs

  By default, the supervised instance won't process any jobs because the `poll_interval` is set to
  `:infinity`. Set the `poll_interval` to a low value to process jobs normally, without manual
  draining.

  ## Options

  Any option accepted by `Oban.start_link/1` is acceptable, including the repo-specific options
  listed in [shared options](#module-shared-repo-options).

  ## Examples

  Start a basic supervised instance without any queues or plugins:

      name = start_supervised_oban!(repo: MyApp.Repo)
      Oban.insert(name, MyWorker.new())

  Start the supervisor with a single queue and polling every 10ms:

      start_supervised_oban!(repo: MyApp.Repo, poll_interval: 10, queues: [alpha: 10])
  """
  @doc since: "0.11.0"
  @spec start_supervised_oban!([Oban.option()]) :: Registry.key()
  def start_supervised_oban!(opts) when is_list(opts) do
    opts =
      @default_supervised_opts
      |> Keyword.merge(opts)
      |> Keyword.put_new(:name, make_ref())

    name = Keyword.fetch!(opts, :name)
    repo = Keyword.fetch!(opts, :repo)

    attach_auto_allow(repo, name)

    start_supervised!({Oban, opts})

    name
  end

  defp json_encode_decode(map) do
    map
    |> Jason.encode!()
    |> Jason.decode!()
  end

  # Callback Helpers

  defp assert_valid_callback(worker, callback) do
    assert is_atom(callback) and callback in @callbacks, """
    Expected callback to be included in #{inspect(@callbacks)}, got: #{inspect(callback)}
    """

    assert Code.ensure_loaded?(worker), """
    Expected worker to be an existing module, got: #{inspect(worker)}
    """

    handler = callback_to_handler(callback)

    assert function_exported?(worker, handler, 1), """
    Expected #{inspect(handler)} callback to be implemented
    """
  end

  defp callback_to_handler(:attempted), do: :handle_attempted
  defp callback_to_handler(:completed), do: :handle_completed
  defp callback_to_handler(:discarded), do: :handle_discarded
  defp callback_to_handler(:exhausted), do: :handle_exhausted

  # Sandbox Helpers

  defp attach_auto_allow(repo, name) do
    telemetry_name = "oban-auto-allow-#{inspect(name)}"

    :telemetry.attach_many(
      telemetry_name,
      [[:oban, :engine, :init, :start], [:oban, :plugin, :init]],
      &__MODULE__.auto_allow/4,
      {name, repo, self()}
    )

    on_exit(name, fn -> :telemetry.detach(telemetry_name) end)
  end

  @doc false
  def auto_allow(_event, _measure, %{conf: conf}, {name, repo, test_pid}) do
    if conf.name == name, do: Sandbox.allow(repo, test_pid, self())
  end

  # Draining Helpers

  defp drain(conf, acc, opts) do
    if opts.with_scheduled, do: stage_scheduled(conf)

    case fetch_available(conf, opts) do
      [_ | _] = jobs ->
        executed =
          Enum.map(jobs, fn job ->
            conf
            |> Executor.new(job, safe: opts.with_safety)
            |> Executor.call()
          end)

        if opts.with_recursion do
          drain(conf, acc ++ executed, opts)
        else
          complete_drain(conf, executed, opts)
        end

      [] ->
        complete_drain(conf, acc, opts)
    end
  end

  defp stage_scheduled(conf) do
    query = where(Job, [j], j.state in ["scheduled", "retryable"])

    Repo.update_all(conf, query, set: [state: "available"])
  end

  defp fetch_available(conf, opts) do
    {:ok, meta} = conf.engine.init(conf, queue: opts.queue, limit: opts.with_limit)
    {:ok, {_meta, jobs}} = conf.engine.fetch_jobs(conf, meta, %{})

    jobs
  end

  defp complete_drain(_conf, executed, %{with_summary: true}) do
    base = %{cancelled: 0, completed: 0, discarded: 0, exhausted: 0, scheduled: 0}

    Enum.reduce(executed, base, fn exec, acc ->
      state =
        case exec.state do
          :cancelled -> :cancelled
          :discard -> :discarded
          :exhausted -> :exhausted
          :failure -> :retryable
          :snoozed -> :scheduled
          :success -> :completed
        end

      Map.update(acc, state, 1, &(&1 + 1))
    end)
  end

  defp complete_drain(conf, executed, _opts) do
    ids =
      executed
      |> Enum.map(& &1.job.id)
      |> Enum.uniq()

    jobs =
      conf
      |> Repo.all(where(Job, [j], j.id in ^ids))
      |> Map.new(fn job -> {job.id, job} end)

    Enum.map(ids, &Map.fetch!(jobs, &1))
  end

  # Chunk Helpers

  @ops ~w(cancel discard error)a

  defp assert_valid_chunk_result(result) do
    valid? =
      case result do
        :ok ->
          true

        {:ok, _value} ->
          true

        {ops, _reason, [_ | _]} when ops in @ops ->
          true

        [{_op, {_reason, _jobs}} | _] = list ->
          Keyword.keyword?(list) and
            Enum.all?(list, &match?({_key, {_reason, [_ | _]}}, &1))

        _ ->
          false
      end

    assert valid?, """
    Expected result to be one of

      - `:ok`
      - `{:ok, value}`
      - `{:cancel, reason, jobs}`
      - `{:discard, reason, jobs}`
      - `{:error, reason, jobs}`
      - `[cancel: {reason, jobs}, discard: {reason, jobs}, error: {reason, jobs}]`

    Instead received:

    #{inspect(result, pretty: true)}
    """
  end
end

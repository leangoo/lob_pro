defmodule Oban.Pro.Plugins.DynamicCron do
  @moduledoc """
  The `DynamicCron` plugin enhances Oban's cron scheduler by making it configurable at runtime,
  globally, across your entire cluster. `DynamicCron` supports adding, updating, deleting, and
  pausing cron entries at boot time _or_ runtime. It is an ideal solution for applications that
  must dynamically start and manage scheduled tasks at runtime.

  ## Installation

  Before running the `DynamicCron` plugin you must run a migration to add the `oban_crons` table
  to your database.

  ```bash
  mix ecto.gen.migration add_oban_crons
  ```

  Open the generated migration in your editor and add a call to the migration's `change/0`
  function:

  ```elixir
  defmodule MyApp.Repo.Migrations.AddObanCron do
    use Ecto.Migration

    defdelegate change, to: Oban.Pro.Migrations.DynamicCron
  end
  ```

  As with the base Oban tables you can optionally provide a `prefix` to "namespace" the table
  within your database. Here we specify a `"private"` prefix:

  ```elixir
  defmodule MyApp.Repo.Migrations.AddObanCron do
    use Ecto.Migration

    def change, do: Oban.Pro.Migrations.DynamicCron.change(prefix: "private")
  end
  ```

  Run the migration to create the table:

  ```bash
  mix ecto.migrate
  ```

  Now you can use the `DynamicCron` plugin and start scheduling periodic jobs!

  ## Using and Configuring

  To begin using `DynamicCron`, add the module to your list of Oban plugins in `config.exs`:

  ```elixir
  config :my_app, Oban,
    plugins: [Oban.Pro.Plugins.DynamicCron]
    ...
  ```

  By itself, without providing a crontab or dynamically inserting cron entries, the plugin doesn't
  have anything to schedule. To get scheduling started, provide a list of `{cron, worker}` or
  `{cron, worker, options}` tuples to the plugin. The syntax is identical to Oban's built in
  `:crontab` option, which means you can copy an existing standard `:crontab` list into the
  plugin's `:crontab`.

  ```elixir
  plugins: [{
    Oban.Pro.Plugins.DynamicCron,
    timezone: "America/Chicago",
    crontab: [
      {"* * * * *", MyApp.MinuteJob},
      {"0 * * * *", MyApp.HourlyJob, queue: :scheduled},
      {"0 0 * * *", MyApp.DailyJob, max_attempts: 1},
      {"0 12 * * MON", MyApp.MondayWorker, tags: ["scheduled"]},
      {"@daily", MyApp.AnotherDailyWorker}
    ]
  }]
  ```

  Now, when dynamic cron initializes, it will persist those cron entries to the database and start
  scheduling them according to their CRON expression. The plugin's `crontab` format is nearly
  identical to Oban's standard crontab, with a few important enhancements we'll look at soon.

  Each of the crontab entries are persisted to the database and referenced globally, by all the
  other connected Oban instances. That allows us to insert, update, or delete cron entries at any
  time. In fact, changing the schedule or options of an entry in the crontab provided to the
  plugin will automatically update the persisted entry. To demonstrate, let's modify the
  `MinuteJob` we specified so that it runs every other minute in the `:scheduled` queue:

  ```elixir
  crontab: [
    {"*/2 * * * *", MyApp.MinuteJob, queue: :scheduled},
    ...
  ]
  ```

  Now it isn't really a "minute job" any more, and the name is no longer suitable. However, we
  didn't provide a name for the entry and it's using the module name instead. To provide more
  flexibility we can add a `:name` overrride, then we can update the worker's name as well:

  ```elixir
  crontab: [
    {"*/2 * * * *", MyApp.FrequentJob, name: "frequent", queue: :scheduled},
    ...
  ]
  ```

  All entries are referenced by name, which defaults to the worker's name and must be unique. You
  may define the same worker multiple times _as long as_ you provide a name override:

  ```elixir
  crontab: [
    {"*/3 * * * *", MyApp.BasicJob, name: "client-1", args: %{client_id: 1}},
    {"*/3 * * * *", MyApp.BasicJob, name: "client-2", args: %{client_id: 2}},
    ...
  ]
  ```

  To temporarily disable scheduling jobs you can set the `paused` flag:


  ```elixir
  crontab: [
    {"* * * * *", MyApp.BasicJob, paused: true},
    ...
  ]
  ```

  To resume the job you must supply `paused: false` (or use `update/2` to resume it manually),
  simply removing the `paused` option will have no effect.

  ```elixir
  crontab: [
    {"* * * * *", MyApp.BasicJob, paused: false},
    ...
  ]
  ```

  It is also possible to delete a persisted entry during initialization by passing the `:delete`
  option:

  ```elixir
  crontab: [
    {"* * * * *", MyApp.MinuteJob, delete: true},
    ...
  ]
  ```

  One or more entries can be deleted this way. Deleting entries is idempotent, nothing will happen
  if no matching entry can be found.

  In the next section we'll look at how to list, insert, update and delete jobs dynamically at
  runtime.

  ## Overriding the Timezone

  Without any configuration the default timezone is `Etc/UTC`. You can override that for all cron
  entries by passing a `timezone` option to the plugin:

  ```elixir
  plugins: [{
    Oban.Pro.Plugins.DynamicCron,
    timezone: "America/Chicago",
    # ...
  ```

  You can also override the timezone for individual entries by passing it as an option to the
  `crontab` list or to `DynamicCron.insert/1`:

  ```elixir
  DynamicCron.insert([
    {"0 0 * * *", MyApp.Pinger, name: "oslo", timezone: "Europe/Oslo"},
    {"0 0 * * *", MyApp.Pinger, name: "chicago", timezone: "America/Chicago"},
    {"0 0 * * *", MyApp.Pinger, name: "zagreb", timezone: "Europe/Zagreb"}
  ])
  ```

  ## Runtime Updates

  Dynamic cron entries are persisted to the database, making it easy to manipulate them through
  typical CRUD operations. The `DynamicCron` plugin provides convenience functions to simplify
  working those operations.

  The `insert/1` function takes a list of one or more tuples with the same `{expression, worker}`
  or `{expression, worker, options}` format as the plugin's `crontab` option:

  ```elixir
  DynamicCron.insert([
    {"0 0 * * *", MyApp.GenericWorker},
    {"* * * * *", MyApp.ClientWorker, name: "client-1", args: %{client_id: 1}},
    {"* * * * *", MyApp.ClientWorker, name: "client-2", args: %{client_id: 2}},
    {"* * * * *", MyApp.ClientWorker, name: "client-3", args: %{client_id: 3}}
  ])
  ```

  Be aware that `insert/1` acts like an "upsert", making it possible to modify existing entries if
  the worker or name matches. Still, it is better to use `update/2` to make targeted updates.

  ## Isolation and Namespacing

  All `DynamicCron` functions have an alternate clause that accepts an Oban instance name as the
  first argument. This is in line with base `Oban` functions such as `Oban.insert/2`, which allow
  you to seamlessly work with multiple Oban instances and across multiple database prefixes. For
  example, you can use `all/1` to list all cron entries for the instance named `ObanPrivate`:

  ```elixir
  entries = DynamicCron.all(ObanPrivate)
  ```

  Likewise, to insert a new entry using the configuration associated with the `ObanPrivate`
  instance:

  ```elixir
  {:ok, _} = DynamicCron.insert(ObanPrivate, [{"* * * * *", PrivateWorker}])
  ```

  ## Instrumenting with Telemetry

  The `DynamicCron` plugin adds the following metadata to the `[:oban, :plugin, :stop]` event:

  * `:jobs` - a list of jobs that were inserted into the database
  """

  @behaviour Oban.Plugin

  use GenServer

  import Ecto.Query, only: [order_by: 2, where: 2]

  alias Ecto.Multi
  alias Oban.Cron.Expression
  alias Oban.Plugins.Cron, as: CronPlugin
  alias Oban.Pro.Cron, as: CronEntry
  alias Oban.{Job, Peer, Repo, Validation, Worker}

  @type cron_expr :: String.t()
  @type cron_name :: String.t() | atom()
  @type cron_opt ::
          {:args, Job.args()}
          | {:expression, cron_expr()}
          | {:max_attempts, pos_integer()}
          | {:paused, boolean()}
          | {:priority, 0..3}
          | {:meta, map()}
          | {:name, cron_name()}
          | {:queue, atom() | String.t()}
          | {:tags, Job.tags()}
          | {:timezone, String.t()}
  @type cron_input :: {cron_expr(), module()} | {cron_expr(), module(), [cron_opt]}

  @type option ::
          {:conf, Oban.Config.t()}
          | {:crontab, [cron_input()]}
          | {:name, Oban.name()}
          | {:timezone, Calendar.time_zone()}
          | {:timeout, timeout()}

  @base_opts [unique: [period: 59]]

  defmodule State do
    @moduledoc false

    defstruct [
      :conf,
      :name,
      :timer,
      crontab: [],
      rebooted: false,
      timeout: :timer.seconds(30),
      timezone: "Etc/UTC"
    ]
  end

  defguardp is_name(name) when is_binary(name) or (is_atom(name) and not is_nil(name))

  @doc false
  def child_spec(args), do: super(args)

  @impl Oban.Plugin
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  @impl Oban.Plugin
  def validate(opts) do
    Validation.validate(opts, fn
      {:conf, _} -> :ok
      {:crontab, crontab} -> Validation.validate(:crontab, crontab, &validate_crontab/1)
      {:name, _} -> :ok
      {:timezone, timezone} -> Validation.validate_timezone(:timezone, timezone)
      {:timeout, timeout} -> Validation.validate_integer(:timeout, timeout)
      option -> {:unknown, option, State}
    end)
  end

  @doc """
  Used to retrieve all persisted cron entries.

  The `all/0` function is provided as a convenience to inspect persisted entries.

  ## Examples

  Return a list of cron schemas with raw attributes:

      entries = DynamicCron.all()
  """
  @spec all(term()) :: [Ecto.Schema.t()]
  def all(oban_name \\ Oban) do
    oban_name
    |> Oban.config()
    |> list_cron()
  end

  @doc """
  Insert cron entries into the database to start scheduling new jobs.

  Be aware that `insert/1` acts like an "upsert", making it possible to modify existing entries if
  the worker or name matches. Still, it is better to use `update/2` to make targeted updates.

  ## Examples

  Insert a list of tuples with the same `{expression, worker}` or `{expression, worker, options}`
  format as the plugin's `crontab` option.

      DynamicCron.insert([
        {"0 0 * * *", MyApp.GenericWorker},
        {"* * * * *", MyApp.ClientWorker, name: "client-1", args: %{client_id: 1}},
        {"* * * * *", MyApp.ClientWorker, name: "client-2", args: %{client_id: 2}},
        {"* * * * *", MyApp.ClientWorker, name: "client-3", args: %{client_id: 3}}
      ])
  """
  @spec insert(term(), [cron_input()]) :: {:ok, [Ecto.Schema.t()]} | {:error, Ecto.Changeset.t()}
  def insert(oban_name \\ Oban, [_ | _] = crontab) do
    conf = Oban.config(oban_name)

    case insert_cron(conf, crontab) do
      {:ok, inserted} ->
        {:ok, Map.values(inserted)}

      {:error, _name, changeset, _changes} ->
        {:error, changeset}
    end
  end

  @doc """
  Update a single cron entry, as identified by worker or name.

  Any option available when specifying an entry in the `crontab` list or when calling `insert/2`
  can be updated—that includes the cron `expression` and the `worker`.

  ## Examples

  The following call demonstrates updating every possible option:

      {:ok, _} =
        DynamicCron.update(
          "cron-1",
          expression: "1 * * * *",
          max_attempts: 10,
          name: "special-cron",
          paused: false,
          priority: 0,
          queue: "dedicated",
          tags: ["client", "scheduled"],
          timezone: "Europe/Amsterdam",
          worker: Other.Worker,
        )

  Naturally, individual options may be updated instead. For example, set `paused: true` to pause
  an entry:

      {:ok, _} = DynamicCron.update(MyApp.ClientWorker, paused: true)

  Since `update/2` operates on a single entry at a time, it is possible to rename an entry without
  doing a `delete`/`insert` dance:

      {:ok, _} = DynamicCron.update(MyApp.ClientWorker, name: "client-worker")

  Or, update an entry with a custom entry name already set:

      {:ok, _} = DynamicCron.update("cron-1", name: "special-cron")
  """
  @spec update(term(), cron_name(), [cron_opt()]) ::
          {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t() | String.t()}
  def update(oban_name \\ Oban, name, opts) when is_name(name) do
    oban_name
    |> Oban.config()
    |> update_cron(Worker.to_string(name), opts)
  end

  @doc """
  Delete individual entries, by worker or name.

  Use `delete/1` to remove entries at runtime, rather than hard-coding the `:delete` flag into the
  `crontab` list at compile time.

  ## Examples

  With the worker as the entry name:

      {:ok, _} = DynamicCron.delete(Worker)

  With a custom name:

      {:ok, _} = DynamicCron.delete("cron-1")
  """
  @spec delete(term(), cron_name()) ::
          {:ok, Ecto.Schema.t()}
          | {:error, Ecto.Changeset.t() | String.t()}
  def delete(oban_name \\ Oban, name) when is_name(name) do
    oban_name
    |> Oban.config()
    |> delete_cron(Worker.to_string(name))
  end

  # Callbacks

  @impl GenServer
  def init(opts) do
    Validation.validate!(opts, &validate/1)

    state = struct!(State, opts)

    :telemetry.execute([:oban, :plugin, :init], %{}, %{conf: state.conf, plugin: __MODULE__})

    {:ok, state, {:continue, :start}}
  end

  @impl GenServer
  def handle_continue(:start, %State{} = state) do
    case insert_cron(state.conf, state.crontab) do
      {:ok, _results} ->
        {:noreply, schedule_evaluate(state)}

      {:error, _name, changeset, _changes} ->
        {:stop, %ArgumentError{message: changeset.errors}, state}
    end
  end

  @impl GenServer
  def handle_info(:evaluate, %State{} = state) do
    meta = %{conf: state.conf, plugin: __MODULE__}

    :telemetry.span([:oban, :plugin], meta, fn ->
      case update_and_insert(state) do
        {:ok, jobs} when is_map(jobs) ->
          {:ok, Map.put(meta, :jobs, Map.values(jobs))}

        error ->
          {:error, Map.put(meta, :error, error)}
      end
    end)

    {:noreply, schedule_evaluate(%{state | rebooted: true})}
  end

  # Validations

  defp validate_crontab({expression, worker, opts}) do
    with {:ok, _} <- CronPlugin.parse(expression) do
      cond do
        not Code.ensure_loaded?(worker) ->
          {:error, "#{inspect(worker)} not found or can't be loaded"}

        not function_exported?(worker, :perform, 1) ->
          {:error, "#{inspect(worker)} does not implement `perform/1` callback"}

        not Keyword.keyword?(opts) ->
          {:error, "options must be a keyword list, got: #{inspect(opts)}"}

        not valid_entry?(opts) ->
          {:error,
           "expected cron options to be one of #{inspect(CronEntry.allowed_opts())}, got: #{inspect(opts)}"}

        not valid_job?(worker, opts) ->
          {:error, "expected valid job options, got: #{inspect(opts)}"}

        true ->
          :ok
      end
    end
  end

  defp validate_crontab({expression, worker}) do
    validate_crontab({expression, worker, []})
  end

  defp validate_crontab(invalid) do
    {:error,
     "expected crontab entry to be an {expression, worker} or " <>
       "{expression, worker, options} tuple, got: #{inspect(invalid)}"}
  end

  defp valid_entry?(opts) do
    string_keys = Enum.map(CronEntry.allowed_opts(), &to_string/1)

    opts
    |> Keyword.drop([:delete, :name, :paused])
    |> Enum.all?(fn {key, _} -> to_string(key) in string_keys end)
  end

  defp valid_job?(worker, opts) do
    args = Keyword.get(opts, :args, %{})
    opts = Keyword.drop(opts, [:args, :delete, :name, :paused, :timezone])

    worker.new(args, opts).valid?
  end

  # Scheduling

  defp schedule_evaluate(state) do
    timer = Process.send_after(self(), :evaluate, CronPlugin.interval_to_next_minute())

    %{state | timer: timer}
  end

  # Updating

  defp update_and_insert(state) do
    if Peer.leader?(state.conf) do
      state
      |> prepare_crontab()
      |> insert_scheduled_jobs()
    else
      {:ok, []}
    end
  end

  defp prepare_crontab(state) do
    crontab =
      for schema <- list_cron(state.conf),
          expr = Expression.parse!(schema.expression),
          not schema.paused,
          not (expr.reboot? and state.rebooted),
          reduce: [] do
        acc ->
          case Worker.from_string(schema.worker) do
            {:ok, worker} ->
              [{schema.name, expr, worker, json_to_keyword(schema.opts)} | acc]

            {:error, _} ->
              acc
          end
      end

    %{state | crontab: crontab}
  end

  defp json_to_keyword(json) do
    Keyword.new(json, fn {key, val} -> {String.to_existing_atom(key), val} end)
  end

  # Inserting

  defp insert_scheduled_jobs(%State{} = state) do
    multi =
      state.crontab
      |> Enum.filter(&now?(&1, state.timezone))
      |> Enum.reduce(Multi.new(), &insert_reducer(&1, &2, state.conf))

    Repo.transaction(state.conf, multi, timeout: state.timeout)
  end

  defp now?({_, cron, _, opts}, default_timezone) do
    {:ok, datetime} =
      opts
      |> Keyword.get(:timezone, default_timezone)
      |> DateTime.now()

    Expression.now?(cron, datetime)
  end

  defp insert_reducer({name, _cron, worker, opts}, multi, conf) do
    {args, opts} = Keyword.pop(opts, :args, %{})

    opts =
      @base_opts
      |> Worker.merge_opts(worker.__opts__())
      |> Worker.merge_opts(opts)
      |> Keyword.delete(:timezone)
      |> Keyword.update(:meta, %{cron_name: name}, &Map.put(&1, :cron_name, name))

    Oban.insert(conf.name, multi, name, worker.new(args, opts))
  end

  # Queries

  defp list_cron(conf) do
    Repo.all(conf, order_by(CronEntry, asc: :inserted_at))
  end

  defp insert_cron(conf, crontab) do
    multi = Enum.reduce(crontab, Multi.new(), &append_operation(conf, &1, &2))

    Repo.transaction(conf, multi)
  end

  defp update_cron(conf, name, params) do
    with {:ok, cron} <- fetch_cron(conf, name) do
      changeset = CronEntry.changeset(cron, Map.new(params))

      Repo.update(conf, changeset)
    end
  end

  defp delete_cron(conf, name) do
    with {:ok, cron} <- fetch_cron(conf, name), do: Repo.delete(conf, cron)
  end

  defp fetch_cron(conf, name) do
    case Repo.one(conf, where(CronEntry, name: ^name)) do
      nil ->
        {:error, "no cron entry named #{inspect(name)} could be found"}

      cron ->
        {:ok, cron}
    end
  end

  defp append_operation(conf, {expr, work}, multi) do
    append_operation(conf, {expr, work, []}, multi)
  end

  defp append_operation(conf, {_expr, worker, opts} = tuple, multi) do
    if Keyword.get(opts, :delete) do
      name =
        opts
        |> Keyword.get(:name, worker)
        |> Worker.to_string()

      Multi.delete_all(multi, name, where(CronEntry, name: ^name), prefix: conf.prefix)
    else
      changeset = CronEntry.changeset(tuple)

      replace =
        changeset.changes
        |> Map.take([:expression, :worker, :opts, :paused])
        |> Map.keys()

      repo_opts = [
        prefix: conf.prefix,
        conflict_target: :name,
        on_conflict: {:replace, replace}
      ]

      Multi.insert(multi, changeset.changes.name, changeset, repo_opts)
    end
  end
end

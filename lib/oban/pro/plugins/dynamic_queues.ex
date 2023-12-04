defmodule Oban.Pro.Plugins.DynamicQueues do
  @moduledoc """
  The `DynamicQueue` plugin extends Oban's basic queue management by persisting queue changes
  across restarts, globally, across all connected nodes. It also boasts a declarative syntax for
  specifying which nodes a queue will run on.

  `DynamicQueues` are ideal for applications that dynamically start, stop, or modify queues at
  runtime.

  ## Installation

  Before running the `DynamicQueues` plugin, you must run a migration to add the `oban_queues`
  table to your database.

  ```bash
  mix ecto.gen.migration add_oban_queues
  ```

  Open the generated migration in your editor and delegate to the dynamic queues migration:

  ```elixir
  defmodule MyApp.Repo.Migrations.AddObanQueues do
    use Ecto.Migration

    defdelegate change, to: Oban.Pro.Migrations.DynamicQueues
  end
  ```

  As with the base Oban tables, you can optionally provide a `prefix` to "namespace" the table
  within your database. Here we specify a `"private"` prefix:

  ```elixir
  defmodule MyApp.Repo.Migrations.AddObanQueues do
    use Ecto.Migration

    def change, do: Oban.Pro.Migrations.DynamicQueues.change(prefix: "private")
  end
  ```

  Run the migration to create the table:

  ```bash
  mix ecto.migrate
  ```

  Now you can use the `DynamicQueues` plugin and start scheduling periodic jobs!

  ## Using and Configuring

  To begin using `DynamicQueues`, add the module to your list of Oban plugins in `config.exs`:

      config :my_app, Oban,
        plugins: [Oban.Pro.Plugins.DynamicQueues]
        ...

  Without providing a list of queues the plugin doesn't have anything to run. The syntax for
  specifying dynamic queues is identical to Oban's built-in `:queues` option, which means you can
  copy them over:

      plugins: [{
        Oban.Pro.Plugins.DynamicQueues,
        queues: [
          default: 20,
          mailers: [global_limit: 20],
          events: [local_limit: 30, rate_limit: [allowed: 100, period: 60]]
        ]
      }]

  > #### Prevent Queue Conflicts {: .warning}
  >
  > Be sure to either omit any top level `queues` configuration to prevent a conflict between
  > basic and dynamic queues.

  Now, when `DynamicQueues` initializes, it will persist all of the queues to the database and
  start supervising them. The `queues` syntax is _nearly_ identical to Oban's standard queues,
  with an important enhancement we'll look at shortly.

  Each of the persisted queues are referenced globally, by all other connected Oban instances that
  are running the `DynamicQueues` plugin. Changing the queue's name, pausing, scaling, or changing
  any other options will automatically update queues across all nodes—and persist across restarts.

  Persisted queues are referenced by name, so you can tweak a queue's options by changing the
  definition within your config. For example, to bump the mailer's global limit up to `30`:

      queues: [
        mailers: [global_limit: 30],
        ...
      ]

  That isn't especially interesting—after all, that's exactly how regular queues work! Dynamic
  queues start to shine when you insert, update, or delete them _dynamically_, either through Oban
  Web or your own application code. But first, let's look at how to limit where dynamic queues run.

  ### Limiting Where Queues Run

  Dynamic queues can be configured to run on a subset of available nodes. This is especially
  useful when wish to restrict resource-intensive queues to only dedicated nodes. Restriction is
  configured through the `only` option, which you use like this:

      queues: [
        basic: [local_limit: 10, only: {:node, :=~, "web|worker"}],
        audio: [local_limit: 5, only: {:node, "worker.1"}],
        video: [local_limit: 5, only: {:node, "worker.2"}],
        learn: [local_limit: 5, only: {:sys_env, "EXLA", "CUDA"}],
        store: [local_limit: 1, only: {:sys_env, "WAREHOUSE", true}]
      ]

  In this example we've defined five queues, with the following restrictions:

  * `basic` — will run on a node named `web` or `worker`
  * `audio` — will only run on a node named `worker.1`
  * `video` — will only run on a node named `worker.2`
  * `learn` — will run wherever `EXLA=CUDA` is an environment variable
  * `store` — will run wherever `WAREHOUSE=true` is an environment variable

  Here are the various match modes, operators, and allowed patterns:

  #### Modes

  * `:node` — matches the node name as set in your Oban config. By default, `node`
    is the node's id in a cluster, the hostname outside a cluster, or a `DYNO`
    variable on Heroku.
  * `:sys_env` — matches a single system environment variable as retrieved by
    `System.get_env/1`

  #### Operators

  * `:==` — compares the pattern and runtime value for _equality_ as strings. This
    is the default operator if nothing is specified.
  * `:!=` — compares the pattern and runtime value for _inequality_ as strings
  * `:=~` — treats the pattern as a regex and matches it against a runtime value

  #### Patterns

  * `boolean` — either `true` or `false`, which is stringified before comparison
  * `string` — either a literal pattern or a regular expression, depending on the
    supplied operator

  ### Deleting Persisted Queues

  It is possible to delete a persisted queue during initialization by passing the `:delete`
  option:

      queues: [
        some_old_queue: [delete: true],
        ...
      ]

  Multiple queues can be deleted simultaneously, if necessary. Deleting queues is also idempotent;
  nothing will happen if a matching queue can't be found.

  In the next section we'll look at how to list, insert, update and delete queues dynamically at
  runtime.

  ## Runtime Updates

  Dynamic queues are persisted to the database, making it easy to manipulate them directly through
  CRUD operations, or indirectly with Oban's queue operations, i.e. `pause_queue/2`,
  `scale_queue/2`.

  Explicit queue options will be overwritten on restart, while omitted fields are retained. For
  example, consider the following dynamic queue entry:

      queues: [
        default: [limit: 10],
        ...
      ]

  Scaling that `default` queue up or down at runtime _wouldn't_ persist across a restart, because
  the definition will overwrite the `limit`. However, pausing the queue or changing the
  `global_limit` _would_ persist because they aren't included in the queue definition.

      # pause, global_limit, etc. will persist, but limit won't
      default: [limit: 10]

      # pause will persist, but limit and global_limit won't
      default: [limit: 10, global_limit: 20]

      # neither limits nor pausing will persist
      default: [limit: 10, global_limit: 20, paused: true]

  See function documentation for `all/0`, `insert/1`, `update/2`, and `delete/1` for more
  information about runtime updates.

  ## Enabling Polling Mode

  In environments with restricted connectivity (where PubSub doesn't work) you can still use
  DynamicQueues at runtime through polling mode. The polling interval is entirely up to you, as
  it's disabled by default.

      config :my_app, Oban,
        plugins: [{Oban.Pro.Plugins.DynamicQueues, interval: :timer.minutes(1)}]

  With the interval above each DynamicQueues instance will wake up every minute, check the
  database for changes, and start new queues.

  ## Isolation and Namespacing

  All DynamicQueues functions have an alternate clause that accepts an Oban instance name for the
  first argument. This matches base `Oban` functions such as `Oban.pause_queue/2`, which allow you
  to seamlessly work with multiple Oban instances and across multiple database prefixes. For
  example, you can use `all/1` to list all queues for the instance named `ObanPrivate`:

      queues = DynamicQueues.all(ObanPrivate)

  Likewise, to insert a new queue using the configuration associated with the `ObanPrivate`
  instance:

      DynamicQueues.insert(ObanPrivate, private: limit: 10)
  """

  @behaviour Oban.Plugin

  use GenServer

  import Ecto.Query, only: [from: 2, order_by: 2, where: 2]

  alias Ecto.{Changeset, Multi}
  alias Oban.Pro.Engines.Smart
  alias Oban.Pro.{Queue, Utils}
  alias Oban.{Config, Midwife, Notifier, Peer, Registry, Repo, Validation}

  @type period :: Smart.period()
  @type partition :: Smart.partition()

  @type operator :: :== | :!= | :=~
  @type pattern :: boolean() | String.t()
  @type sys_key :: String.t()
  @type oban_name :: term()

  @type queue_name :: atom() | binary()
  @type queue_input :: [{queue_name(), pos_integer() | queue_opts() | [queue_opts()]}]
  @type queue_opts ::
          {:local_limit, pos_integer()}
          | {:global_limit, Smart.global_limit()}
          | {:only, only()}
          | {:paused, boolean()}
          | {:rate_limit, Smart.rate_limit()}

  @type only ::
          {:node, pattern()}
          | {:node, operator(), pattern()}
          | {:sys_env, sys_key(), pattern()}
          | {:sys_env, sys_key(), operator(), pattern()}

  defmodule State do
    @moduledoc false

    @enforce_keys [:conf]
    defstruct [
      :conf,
      :name,
      :timer,
      interval: :infinity,
      queues: []
    ]
  end

  defguardp is_name(name) when is_binary(name) or (is_atom(name) and not is_nil(name))

  @impl Oban.Plugin
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  @impl Oban.Plugin
  def validate(opts) do
    Validation.validate(opts, fn
      {:conf, conf} -> validate_engine(conf)
      {:name, _} -> :ok
      {:interval, interval} -> Validation.validate_timeout(:interval, interval)
      {:queues, queues} -> validate_queues(queues)
      option -> {:unknown, option, State}
    end)
  end

  @doc """
  Retrieve all persisted queues.

  While it's possible to modify queue's returned from `all/0`, it is recommended that you use
  `update/2` to ensure options are cast and validated correctly.

  ## Examples

  Retrieve a list of all queue schemas with persisted attributes:

      DynamicQueues.all()
  """
  @spec all(oban_name()) :: [Ecto.Schema.t()]
  def all(oban_name \\ Oban) do
    oban_name
    |> Oban.config()
    |> all_queues()
  end

  @doc """
  Persist a list of queue inputs, exactly like the `:queues` option passed as configuration.

  Note that `insert/1` acts like an upsert, making it possible to modify queues if the name
  matches. Still, it is better to use `update/2` to make targeted updates.

  ## Examples

  Insert a variety of queues with standard and advanced options:

      DynamicQueues.insert(
        basic: 10,
        audio: [global_limit: 10],
        video: [global_limit: 10],
        learn: [local_limit: 5, only: {:node, :=~, "learn"}]
      )
  """
  @spec insert(oban_name(), queue_input()) ::
          {:ok, [Ecto.Schema.t()]} | {:error, Ecto.Changeset.t()}
  def insert(oban_name \\ Oban, [_ | _] = entries) do
    conf = Oban.config(oban_name)

    conf
    |> insert_queues(entries)
    |> case do
      {:ok, changes} ->
        inserted = Map.values(changes)

        Enum.each(inserted, &notify(conf, :dyn_start, &1.name))

        {:ok, inserted}

      {:error, _name, changeset, _changes} ->
        {:error, changeset}
    end
  end

  @doc """
  Modify a single queue's options.

  Every option available when inserting queues can be updated.

  ## Examples

  The following call demonstrates updating every possible option:

      DynamicQueues.update(
        :video,
        local_limit: 5,
        global_limit: 20,
        rate_limit: [allowed: 10, period: 30, partition: [fields: [:worker]]],
        only: {:node, :=~, "media"},
        paused: false
      )

  Updating a single option won't remove other persisted options. If you'd like to
  clear an uption you must set them to `nil`:

      DynamicQueues.update(:video, global_limit: nil)

  Since `update/2` operates on a single queue, it is possible to rename a queue
  without doing a `delete`/`insert` dance:

      DynamicQueues.update(:video, name: :media)
  """
  @spec update(oban_name(), queue_name(), queue_opts()) ::
          {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  def update(oban_name \\ Oban, name, opts) when is_name(name) do
    conf = Oban.config(oban_name)

    with {:ok, queue} <- fetch_queue(conf, to_string(name)),
         {:ok, queue} <- update_queue(conf, queue, opts) do
      if queue.name == to_string(name) do
        Oban.scale_queue(oban_name, Keyword.put(opts, :queue, name))
      else
        notify(conf, :dyn_start, queue.name)
        notify(conf, :dyn_stop, name)
      end

      {:ok, queue}
    end
  end

  defp update_queue(conf, queue, opts) do
    params =
      case Keyword.pop(opts, :name) do
        {nil, opts} ->
          %{opts: Map.new(opts)}

        {name, []} ->
          %{name: to_string(name)}

        {name, opts} ->
          %{name: to_string(name), opts: Map.new(opts)}
      end

    changeset = Queue.changeset(queue, params)

    Repo.update(conf, changeset)
  end

  @doc """
  Delete a queue by name at runtime, rather than using the `:delete` option into the `queues` list
  in your configuration.

  ## Examples

  Delete ethe "audio" queue:

      {:ok, _} = DynamicQueues.delete(:audio)
  """
  @spec delete(oban_name(), queue_name()) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  def delete(oban_name \\ Oban, name) when is_name(name) do
    conf = Oban.config(oban_name)

    with {:ok, queue} <- fetch_queue(conf, to_string(name)),
         {:ok, queue} <- Repo.delete(conf, queue) do
      notify(conf, :dyn_stop, queue.name)

      {:ok, queue}
    end
  end

  # Callbacks

  @impl GenServer
  def init(opts) do
    Validation.validate!(opts, &validate/1)

    state = struct!(State, opts)

    :ok = Notifier.listen(state.conf.name, [:signal])

    :telemetry.execute([:oban, :plugin, :init], %{}, %{conf: state.conf, plugin: __MODULE__})

    {:ok, state, {:continue, :start}}
  end

  @impl GenServer
  def handle_continue(:start, %State{} = state) do
    case insert_queues(state.conf, state.queues) do
      {:ok, _} ->
        state.conf
        |> all_queues()
        |> Enum.reject(&queue_running?(state.conf, &1))
        |> Enum.each(&start_queue(state.conf, &1))

        {:noreply, schedule_refresh(state)}

      {:error, _name, changeset, _changes} ->
        {:stop, %ArgumentError{message: changeset.errors}, state}
    end
  end

  @impl GenServer
  def handle_info({:notification, :signal, payload}, %State{} = state) do
    case payload do
      %{"action" => "scale", "ident" => "any", "queue" => queue_name} ->
        scale_queue(state.conf, queue_name, payload)

      %{"action" => "pause", "ident" => "any", "queue" => queue_name} ->
        scale_queue(state.conf, queue_name, %{"paused" => true})

      %{"action" => "resume", "ident" => "any", "queue" => queue_name} ->
        scale_queue(state.conf, queue_name, %{"paused" => false})

      %{"action" => "dyn_start", "queue" => queue_name} ->
        start_queue(state.conf, queue_name)

      %{"action" => "dyn_stop", "queue" => queue_name} ->
        stop_queue(state.conf, queue_name)

      _ ->
        :ignore
    end

    {:noreply, state}
  end

  def handle_info(:refresh, %State{} = state) do
    for queue <- all_queues(state.conf), not queue_running?(state.conf, queue) do
      start_queue(state.conf, queue)
    end

    {:noreply, schedule_refresh(state)}
  end

  # Validations

  defp validate_engine(%Config{engine: engine}) do
    if engine == Smart do
      :ok
    else
      {:error,
       """
       DynamicQueues requires the Smart engine to run correctly, but you're using:

       engine: #{inspect(engine)}

       You can either use the Smart engine or remove DynamicQueue from your plugins.
       """}
    end
  end

  defp validate_queues(queues) do
    Validation.validate(:queues, queues, &validate_queue/1)
  end

  defp validate_queue({_name, delete: true}), do: :ok

  defp validate_queue({name, _} = tuple) when is_atom(name) do
    changeset = Queue.changeset(tuple)

    if changeset.valid? do
      :ok
    else
      exception = Utils.to_exception(changeset)

      {:error, "expected #{inspect(name)} to be a valid queue, but: #{exception.message}"}
    end
  end

  defp validate_queue(queue) do
    {:error, "expected queue to be a keyword pair, got: #{inspect(queue)}"}
  end

  # Queries

  defp all_queues(conf) do
    Repo.all(conf, order_by(Queue, asc: :inserted_at))
  end

  defp insert_queues(conf, queues) do
    multi = Enum.reduce(queues, Multi.new(), &append_operation(conf, &1, &2))

    Repo.transaction(conf, multi)
  end

  defp append_operation(conf, {name, [delete: true]}, multi) do
    Multi.delete_all(multi, name, where(Queue, name: ^to_string(name)), prefix: conf.prefix)
  end

  defp append_operation(conf, tuple, multi) do
    changeset = Queue.changeset(tuple)

    opts_json =
      changeset
      |> Changeset.get_embed(:opts, :struct)
      |> Ecto.embedded_dump(:json)
      |> Enum.reject(fn {_key, val} -> is_nil(val) end)
      |> Map.new()

    on_conflict =
      if Changeset.changed?(changeset, :only) do
        from q in Queue,
          update: [
            set: [opts: fragment("? || ?", q.opts, ^opts_json)],
            set: [only: ^Changeset.get_embed(changeset, :only, :struct)]
          ]
      else
        from q in Queue, update: [set: [opts: fragment("? || ?", q.opts, ^opts_json)]]
      end

    repo_opts = [
      prefix: conf.prefix,
      conflict_target: :name,
      on_conflict: on_conflict
    ]

    Multi.insert(multi, changeset.changes.name, changeset, repo_opts)
  end

  defp fetch_queue(conf, name) do
    case Repo.one(conf, where(Queue, name: ^name)) do
      nil -> {:error, "no queue named #{inspect(name)} could be found"}
      queue -> {:ok, queue}
    end
  end

  # Scheduling

  defp schedule_refresh(%State{interval: interval} = state) do
    case interval do
      :infinity ->
        state

      _ ->
        %{state | timer: Process.send_after(self(), :refresh, state.interval)}
    end
  end

  # Notification

  defp notify(conf, action, queue_name) do
    Notifier.notify(conf.name, :signal, %{action: action, ident: :any, queue: queue_name})
  end

  # Supervision

  defp queue_running?(conf, queue) do
    conf.name
    |> Registry.whereis({:supervisor, queue.name})
    |> is_pid()
  end

  defp scale_queue(conf, queue_name, payload) do
    with true <- Peer.leader?(conf),
         {:ok, queue} <- fetch_queue(conf, queue_name) do
      new_opts = Map.drop(payload, ["action", "ident", "queue"])

      {:ok, _} = Repo.update(conf, Queue.changeset(queue, %{opts: new_opts}))
    end
  end

  defp start_queue(conf, queue_name) when is_binary(queue_name) do
    with {:ok, queue} <- fetch_queue(conf, queue_name), do: start_queue(conf, queue)
  end

  defp start_queue(conf, queue) do
    if run_locally?(conf, queue.only) do
      Midwife.start_queue(conf, Queue.to_keyword_opts(queue))
    else
      :ignore
    end
  end

  defp stop_queue(conf, queue_name) when is_binary(queue_name) do
    Midwife.stop_queue(conf, queue_name)
  end

  defp run_locally?(%{node: node}, %{mode: :node, op: op, value: value}) do
    compare(node, op, value)
  end

  defp run_locally?(_conf, %{mode: :sys_env, op: op, key: key, value: value}) do
    key
    |> System.get_env()
    |> to_string()
    |> compare(op, value)
  end

  defp run_locally?(_conf, _only), do: true

  defp compare(value_a, :==, value_b), do: value_a == value_b
  defp compare(value_a, :!=, value_b), do: value_a != value_b
  defp compare(value_a, :=~, value_b), do: value_a =~ Regex.compile!(value_b, "i")
end

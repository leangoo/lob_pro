defmodule Oban.Pro.Plugins.DynamicQueues do
  @moduledoc false

  @behaviour Oban.Plugin

  use GenServer

  import Ecto.Query, only: [order_by: 2, where: 2]

  alias Ecto.Multi
  alias Oban.Pro.Queue.SmartEngine
  alias Oban.Pro.{Queue, Utils}
  alias Oban.Queue.Supervisor, as: QueueSupervisor
  alias Oban.{Config, Notifier, Peer, Registry, Repo, Validation}

  @type oban_name :: term()
  @type queue_name :: String.t() | atom()
  @type queue_opts :: Keyword.t()
  @type queue_input :: [{queue_name(), pos_integer() | queue_opts()}]

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
      option -> {:error, "unknown option provided: #{inspect(option)}"}
    end)
  end

  @spec all(oban_name()) :: [Ecto.Schema.t()]
  def all(oban_name \\ Oban) do
    oban_name
    |> Oban.config()
    |> all_queues()
  end

  @spec insert(oban_name(), [queue_input()]) ::
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

  @spec update(oban_name(), queue_name(), queue_opts()) ::
          {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  def update(oban_name \\ Oban, name, opts) when is_name(name) do
    conf = Oban.config(oban_name)

    with {:ok, queue} <- fetch_queue(conf, to_string(name)),
         {:ok, queue} <- update_queue(conf, queue, opts) do
      if queue.name != to_string(name) do
        :ok = notify(conf, :dyn_start, queue.name)
        :ok = notify(conf, :dyn_stop, name)
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
    if engine == SmartEngine do
      :ok
    else
      {:error,
       """
       The DynamicQueues plugin requires the SmartEngine to run correctly, but you're using:

       engine: #{inspect(engine)}

       You can either use the SmartEngine or remove DynamicQueue from your plugins.
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

    replace =
      changeset.changes
      |> Map.take([:only, :opts])
      |> Map.keys()

    repo_opts = [
      prefix: conf.prefix,
      conflict_target: :name,
      on_conflict: {:replace, replace}
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
      spec = queue_spec(conf, queue)

      conf.name
      |> Registry.via()
      |> Supervisor.start_child(spec)
    else
      :ignore
    end
  end

  defp stop_queue(conf, queue_name) when is_binary(queue_name) do
    %{id: child_id} = queue_spec(conf, %{name: queue_name, opts: %{}})

    Supervisor.terminate_child(Registry.via(conf.name), child_id)
    Supervisor.delete_child(Registry.via(conf.name), child_id)
  end

  defp queue_spec(conf, queue) do
    QueueSupervisor.child_spec({queue.name, Queue.to_keyword_opts(queue)}, conf)
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

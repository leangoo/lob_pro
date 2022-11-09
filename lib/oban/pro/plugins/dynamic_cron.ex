defmodule Oban.Pro.Plugins.DynamicCron do
  @moduledoc false

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
          | {:name, cron_name()}
          | {:queue, atom() | String.t()}
          | {:tags, Job.tags()}
          | {:timezone, String.t()}
  @type cron_input :: {cron_expr(), module()} | {cron_expr(), module(), [cron_opt]}

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

  @impl Oban.Plugin
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
      option -> {:error, "unknown option provided: #{inspect(option)}"}
    end)
  end

  @spec all(term()) :: [Ecto.Schema.t()]
  def all(oban_name \\ Oban) do
    oban_name
    |> Oban.config()
    |> list_cron()
  end

  @spec insert(term(), [cron_input()]) :: {:ok, [Ecto.Schema.t()]} | {:error, Ecto.Changeset.t()}
  def insert(oban_name \\ Oban, [_ | _] = crontab) do
    oban_name
    |> Oban.config()
    |> insert_cron(crontab)
    |> case do
      {:ok, inserted} ->
        {:ok, Map.values(inserted)}

      {:error, _name, changeset, _changes} ->
        {:error, changeset}
    end
  end

  @spec update(term(), cron_name(), [cron_opt()]) ::
          {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  def update(oban_name \\ Oban, name, opts) when is_name(name) do
    oban_name
    |> Oban.config()
    |> update_cron(Worker.to_string(name), opts)
  end

  @spec delete(term(), cron_name()) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
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

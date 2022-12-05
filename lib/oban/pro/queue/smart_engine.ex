require Protocol

Protocol.derive(Jason.Encoder, Oban.Pro.Producer.Meta.GlobalLimit)
Protocol.derive(Jason.Encoder, Oban.Pro.Producer.Meta.GlobalLimit.Partition)
Protocol.derive(Jason.Encoder, Oban.Pro.Producer.Meta.RateLimit)
Protocol.derive(Jason.Encoder, Oban.Pro.Producer.Meta.RateLimit.Partition)

defmodule Oban.Pro.Queue.SmartEngine do
  @moduledoc false

  @behaviour Oban.Engine

  import Ecto.Query
  import DateTime, only: [utc_now: 0]

  alias Ecto.{Changeset, Multi}
  alias Oban.{Backoff, Config, Engine, Job, Repo}
  alias Oban.Engines.Basic
  alias Oban.Pro.Limiters.{Global, Rate}
  alias Oban.Pro.{Producer, Utils}

  @base_batch_size 1_000
  @uniq_batch_size 250

  defmacrop dec_tracked_count(meta, key) do
    quote do
      fragment(
        "jsonb_set(?, ?, ((? #> ?)::text::int - 1)::text::jsonb)",
        unquote(meta),
        ["global_limit", "tracked", unquote(key), "count"],
        unquote(meta),
        ["global_limit", "tracked", unquote(key), "count"]
      )
    end
  end

  defmacrop contains?(column, map) do
    quote do
      fragment("? @> ?", unquote(column), unquote(map))
    end
  end

  defmacrop contained_in?(column, map) do
    quote do
      fragment("? <@ ?", unquote(column), unquote(map))
    end
  end

  @impl Engine
  def init(%Config{} = conf, [_ | _] = opts) do
    {validate?, opts} = Keyword.pop(opts, :validate, false)
    {prod_opts, meta_opts} = Keyword.split(opts, [:queue, :refresh_interval, :updated_at])

    changeset =
      prod_opts
      |> Keyword.put(:name, conf.name)
      |> Keyword.put(:node, conf.node)
      |> Keyword.put(:meta, meta_opts)
      |> Keyword.put_new(:started_at, utc_now())
      |> Keyword.put_new(:updated_at, utc_now())
      |> Keyword.put_new(:queue, :default)
      |> Producer.new()

    case Changeset.apply_action(changeset, :insert) do
      {:ok, producer} ->
        if validate? do
          {:ok, producer}
        else
          with_retry(producer.meta, fn ->
            {:ok, inserted} = Repo.insert(conf, changeset)

            put_producer(conf, inserted)

            {:ok, inserted}
          end)
        end

      {:error, changeset} ->
        {:error, Utils.to_exception(changeset)}
    end
  end

  @impl Engine
  def refresh(%Config{} = conf, %Producer{name: name, queue: queue} = producer) do
    now = utc_now()
    outdated_at = interval_multiple(now, producer, 2)
    vanished_at = interval_multiple(now, producer, 120)

    update_query =
      Producer
      |> where([p], p.uuid == ^producer.uuid)
      |> update([p], set: [updated_at: ^now])

    delete_query =
      Producer
      |> where([p], p.uuid != ^producer.uuid and p.updated_at <= ^vanished_at)
      |> or_where(
        [p],
        p.uuid != ^producer.uuid and
          p.name == ^name and
          p.queue == ^queue and
          p.updated_at <= ^outdated_at
      )

    with_retry(producer.meta, fn ->
      Repo.transaction(conf, fn ->
        Repo.update_all(conf, update_query, [])
        Repo.delete_all(conf, delete_query)
      end)

      %{producer | updated_at: now}
    end)
  end

  defp interval_multiple(now, producer, mult) do
    time = Backoff.jitter(-mult * producer.refresh_interval, mode: :inc)

    DateTime.add(now, time, :millisecond)
  end

  @impl Engine
  def shutdown(%Config{} = conf, %Producer{} = producer) do
    put_meta(conf, producer, :paused, true)
  catch
    :exit, _reason ->
      producer
      |> Producer.update_meta(:paused, true)
      |> Changeset.apply_action!(:update)
  end

  @impl Engine
  def put_meta(%Config{} = conf, %Producer{uuid: uuid}, key, value) do
    {:ok, producer} =
      Repo.transaction(conf, fn ->
        query =
          Producer
          |> where([p], p.uuid == ^uuid)
          |> lock("FOR UPDATE")

        changeset =
          conf
          |> Repo.one(query)
          |> Producer.update_meta(key, value)

        {:ok, producer} = Repo.update(conf, changeset)

        put_producer(conf, producer)

        producer
      end)

    producer
  end

  @impl Engine
  def check_meta(_conf, %Producer{} = producer, _running) do
    meta =
      producer.meta
      |> Map.from_struct()
      |> flatten_windows()

    producer
    |> Map.take([:name, :node, :queue, :started_at, :updated_at])
    |> Map.merge(meta)
    |> Map.put(:running, producer.running_ids)
  end

  defp flatten_windows(%{rate_limit: %{windows: windows}} = meta) do
    {pacc, cacc} =
      Enum.reduce(windows, {0, 0}, fn map, {pacc, cacc} ->
        %{"prev_count" => prev, "curr_count" => curr} = map

        {pacc + prev, cacc + curr}
      end)

    put_in(meta.rate_limit.windows, [%{"curr_count" => cacc, "prev_count" => pacc}])
  end

  defp flatten_windows(meta), do: meta

  @impl Engine
  def fetch_jobs(_conf, %{meta: %{paused: true}} = prod, _running) do
    {:ok, {prod, []}}
  end

  def fetch_jobs(_conf, %{meta: %{local_limit: limit}} = prod, running)
      when map_size(running) >= limit do
    {:ok, {prod, []}}
  end

  def fetch_jobs(%Config{} = conf, %Producer{} = producer, running) do
    multi =
      Multi.new()
      |> Multi.put(:conf, conf)
      |> Multi.put(:running, running)
      |> Multi.run(:producer, &refresh_producer(&1, &2, producer))
      |> Multi.run(:mutex, &take_lock/2)
      |> Multi.run(:local_demand, &check_local_demand/2)
      |> Multi.run(:global_demand, &Global.check/2)
      |> Multi.run(:rate_demand, &Rate.check/2)
      |> Multi.run(:jobs, &fetch_jobs/2)
      |> Multi.run(:tracked_producer, &track_jobs/2)

    with_retry(producer.meta, fn ->
      case Repo.transaction(conf, multi) do
        {:ok, %{tracked_producer: prod, jobs: jobs}} ->
          {:ok, {prod, jobs}}

        {:error, :mutex, :locked, _} ->
          jittery_sleep()

          fetch_jobs(conf, producer, running)
      end
    end)
  end

  @impl Engine
  def complete_job(%Config{} = conf, %Job{} = job) do
    set = [state: "completed", completed_at: utc_now()]

    set =
      receive do
        {:record_meta, meta} -> Keyword.put(set, :meta, meta)
      after
        0 -> set
      end

    Multi.new()
    |> Multi.put(:conf, conf)
    |> Multi.put(:job, job)
    |> Multi.update_all(:set, where(Job, id: ^job.id), [set: set], oper_opts(conf))
    |> Multi.run(:ack, &ack_job/2)
    |> transaction(conf)

    :ok
  end

  @impl Engine
  def discard_job(%Config{} = conf, %Job{} = job) do
    updates = [
      set: [state: "discarded", discarded_at: utc_now()],
      push: [errors: %{attempt: job.attempt, at: utc_now(), error: format_blamed(job)}]
    ]

    Multi.new()
    |> Multi.put(:conf, conf)
    |> Multi.put(:job, job)
    |> Multi.update_all(:set, where(Job, id: ^job.id), updates, oper_opts(conf))
    |> Multi.run(:ack, &ack_job/2)
    |> transaction(conf)

    :ok
  end

  @impl Engine
  def error_job(%Config{} = conf, %Job{} = job, seconds) do
    set =
      if job.attempt >= job.max_attempts do
        [state: "discarded", discarded_at: utc_now()]
      else
        [state: "retryable", scheduled_at: seconds_from_now(seconds)]
      end

    updates = [
      set: set,
      push: [errors: %{attempt: job.attempt, at: utc_now(), error: format_blamed(job)}]
    ]

    Multi.new()
    |> Multi.put(:conf, conf)
    |> Multi.put(:job, job)
    |> Multi.update_all(:set, where(Job, id: ^job.id), updates, oper_opts(conf))
    |> Multi.run(:ack, &ack_job/2)
    |> transaction(conf)

    :ok
  end

  @impl Engine
  def snooze_job(%Config{} = conf, %Job{} = job, seconds) do
    updates = [
      set: [state: "scheduled", scheduled_at: seconds_from_now(seconds)],
      inc: [max_attempts: 1]
    ]

    Multi.new()
    |> Multi.put(:conf, conf)
    |> Multi.put(:job, job)
    |> Multi.update_all(:set, where(Job, id: ^job.id), updates, oper_opts(conf))
    |> Multi.run(:ack, &ack_job/2)
    |> transaction(conf)

    :ok
  end

  @impl Engine
  def cancel_job(%Config{} = conf, %Job{} = job) do
    updates = [set: [state: "cancelled", cancelled_at: utc_now()]]

    updates =
      if is_map(job.unsaved_error) do
        error = %{attempt: job.attempt, at: utc_now(), error: format_blamed(job)}

        Keyword.put(updates, :push, errors: error)
      else
        updates
      end

    query =
      Job
      |> where(id: ^job.id)
      |> where([j], j.state not in ["completed", "discarded", "cancelled"])
      |> select([:id, :args, :attempted_by, :queue, :worker])

    Multi.new()
    |> Multi.put(:conf, conf)
    |> Multi.update_all(:jobs, query, updates, oper_opts(conf))
    |> Multi.run(:ack, &ack_cancelled_jobs/2)
    |> transaction(conf)

    :ok
  end

  @impl Engine
  def cancel_all_jobs(%Config{} = conf, queryable) do
    all_executing = fn _repo, _changes ->
      query =
        queryable
        |> where(state: "executing")
        |> select([:id, :args, :attempted_by, :queue, :worker])

      {:ok, {0, Repo.all(conf, query)}}
    end

    cancel_query =
      queryable
      |> where([j], j.state not in ["completed", "discarded", "cancelled"])
      |> update([j], set: [state: "cancelled", cancelled_at: ^utc_now()])

    {:ok, %{cancelled: {count, _}, jobs: {_, executing}}} =
      Multi.new()
      |> Multi.put(:conf, conf)
      |> Multi.run(:jobs, all_executing)
      |> Multi.run(:ack, &ack_cancelled_jobs/2)
      |> Multi.update_all(:cancelled, cancel_query, [], oper_opts(conf))
      |> transaction(conf)

    {:ok, {count, executing}}
  end

  @impl Engine
  def insert_job(conf, changeset, opts) do
    if changeset.valid? do
      case insert_all_jobs(conf, [changeset], opts) do
        [job] ->
          {:ok, job}

        _ ->
          {:error, changeset}
      end
    else
      {:error, changeset}
    end
  end

  @impl Engine
  def insert_job(%Config{} = conf, %Multi{} = multi, name, fun, opts) when is_function(fun, 1) do
    Multi.run(multi, name, fn repo, changes ->
      insert_job(%{conf | repo: repo}, fun.(changes), opts)
    end)
  end

  @impl Engine
  def insert_job(%Config{} = conf, %Multi{} = multi, name, changeset, opts) do
    Multi.run(multi, name, fn repo, _changes ->
      insert_job(%{conf | repo: repo}, changeset, opts)
    end)
  end

  @impl Engine
  def insert_all_jobs(conf, changesets, opts) do
    xact_limit = Keyword.get(opts, :xact_limit, 10_000)

    changesets = Basic.expand(changesets, %{})

    if Enum.count(changesets, &unique?/1) < xact_limit do
      {:ok, jobs} =
        Repo.transaction(conf, fn ->
          inner_insert_all(conf, changesets, opts)
        end)

      jobs
    else
      inner_insert_all(conf, changesets, opts)
    end
  end

  defp inner_insert_all(conf, changesets, opts) do
    batch_size =
      Keyword.get_lazy(opts, :batch_size, fn ->
        if Enum.any?(changesets, &unique?/1), do: @uniq_batch_size, else: @base_batch_size
      end)

    changesets
    |> Enum.map(&with_uniq_key/1)
    |> Enum.uniq_by(&get_uniq_key(&1, System.unique_integer()))
    |> Enum.chunk_every(batch_size)
    |> Enum.flat_map(fn changesets ->
      uniques = Enum.filter(changesets, &unique?/1)

      {:ok, %{all_jobs: jobs}} =
        Multi.new()
        |> Multi.put(:conf, conf)
        |> Multi.put(:opts, opts)
        |> Multi.put(:changesets, changesets)
        |> Multi.put(:uniques, uniques)
        |> Multi.run(:xact_set, &take_uniq_locks/2)
        |> Multi.run(:dupe_map, &find_uniq_dupes/2)
        |> Multi.run(:new_jobs, &insert_entries/2)
        |> Multi.run(:rep_jobs, &apply_replacements/2)
        |> Multi.run(:all_jobs, &apply_conflicts/2)
        |> transaction(conf, opts)

      jobs
    end)
  end

  defp unique?(%{changes: changes}) do
    is_map_key(changes, :unique) and is_map(changes.unique)
  end

  @impl Engine
  def insert_all_jobs(%Config{} = conf, %Multi{} = multi, name, wrapper, opts) do
    Multi.run(multi, name, fn repo, changes ->
      conf = %{conf | repo: repo}

      {:ok, insert_all_jobs(conf, Basic.expand(wrapper, changes), opts)}
    end)
  end

  @impl Engine
  defdelegate retry_job(conf, job), to: Basic

  @impl Engine
  defdelegate retry_all_jobs(conf, queryable), to: Basic

  # Producer Fetching Helpers

  defp put_producer(conf, producer) do
    Registry.put_meta(Oban.Registry, via(conf, producer.queue), producer)
  end

  defp get_producer(conf, job) do
    with :error <- get_producer_from_registry(conf, job),
         :error <- get_producer_from_database(conf, job),
         [_node, uuid] <- job.attempted_by do
      %Producer{uuid: uuid}
    end
  end

  defp get_producer_from_registry(conf, job) do
    with {:ok, producer} <- Registry.meta(Oban.Registry, via(conf, job.queue)) do
      producer
    end
  end

  defp get_producer_from_database(_conf, %{attempted_by: nil}), do: :error

  defp get_producer_from_database(conf, %{attempted_by: [_node, uuid]}) do
    with nil <- Repo.one(conf, where(Producer, [p], p.uuid == ^uuid)) do
      :error
    end
  end

  # Refreshing is only necessary for global_limit. To avoid the extra data
  # transfer, we only refresh for global_limit queues.
  defp refresh_producer(_repo, %{conf: conf}, producer) do
    if is_map(producer.meta.global_limit) do
      query =
        Producer
        |> where([p], p.uuid == ^producer.uuid)
        |> lock("FOR UPDATE")

      case Repo.one(conf, query) do
        nil -> {:error, :producer_missing}
        producer -> {:ok, producer}
      end
    else
      {:ok, producer}
    end
  end

  defp via(%Config{name: name}, queue) do
    Oban.Registry.via(name, {:producer, queue})
  end

  # Demand Helpers

  defp check_local_demand(_repo, %{producer: producer, running: running}) do
    {:ok, producer.meta.local_limit - map_size(running)}
  end

  # Fetch Helpers

  defp take_lock(_repo, %{conf: conf, producer: %{meta: meta, queue: queue}}) do
    if is_map(meta.global_limit) or is_map(meta.rate_limit) do
      lock_key = :erlang.phash2([conf.prefix, queue])

      case Repo.query(conf, "SELECT pg_try_advisory_xact_lock($1)", [lock_key]) do
        {:ok, %{rows: [[true]]}} -> {:ok, true}
        _ -> {:error, :locked}
      end
    else
      {:ok, true}
    end
  end

  defp fetch_jobs(_repo, %{conf: conf, producer: producer} = changes) do
    query =
      Job
      |> where([j], j.id in subquery(fetch_subquery(changes)))
      |> select([j, _], j)

    updates = [
      set: [state: "executing", attempted_at: utc_now(), attempted_by: [conf.node, producer.uuid]],
      inc: [attempt: 1]
    ]

    options =
      if partitioned?(changes) do
        [prepare: :unnamed]
      else
        []
      end

    case Repo.update_all(conf, query, updates, options) do
      {0, nil} -> {:ok, []}
      {_count, jobs} -> {:ok, jobs}
    end
  end

  defp fetch_subquery(%{local_demand: local, producer: producer} = changes) do
    case changes do
      %{global_demand: nil, rate_demand: nil} ->
        fetch_subquery(producer, local)

      %{global_demand: global, rate_demand: nil} when is_integer(global) ->
        fetch_subquery(producer, min(local, global))

      %{global_demand: nil, rate_demand: rated} when is_integer(rated) ->
        fetch_subquery(producer, min(local, rated))

      %{global_demand: global, rate_demand: [_ | _] = demands} ->
        limiter = producer.meta.rate_limit

        fetch_subquery(producer, limiter, min(local, global || local), demands)

      %{global_demand: [_ | _] = demands, rate_demand: rated} ->
        limiter = producer.meta.global_limit

        fetch_subquery(producer, limiter, min(local, rated || local), demands)

      %{global_demand: global, rate_demand: rated} ->
        limit =
          rated
          |> min(local)
          |> min(global)

        fetch_subquery(producer, limit)
    end
  end

  defp fetch_subquery(producer, limit) do
    base =
      if producer.queue == "__all__" do
        where(Job, state: "available")
      else
        where(Job, state: "available", queue: ^producer.queue)
      end

    base
    |> order_by(asc: :priority, asc: :scheduled_at, asc: :id)
    |> limit(^max(limit, 0))
    |> lock("FOR UPDATE SKIP LOCKED")
    |> select([:id])
  end

  defp fetch_subquery(producer, limiter, limit, demands) do
    if Enum.all?(demands, &(elem(&1, 0) >= limit)) do
      fetch_subquery(producer, limit)
    else
      partition = limiter.partition
      partition_by = partition_by_fields(partition)
      order_by = [asc: :priority, asc: :scheduled_at, asc: :id]

      from_query =
        Job
        |> where(state: "available", queue: ^producer.queue)
        |> order_by(^order_by)
        |> limit(^max(limit * 10, 100))

      partitioned_query =
        from_query
        |> select([j], %{id: j.id, priority: j.priority, scheduled_at: j.scheduled_at})
        |> select_merge([j], %{worker: j.worker, args: j.args})
        |> select_merge([j], %{rank: over(dense_rank(), :partition)})
        |> windows([j], partition: [partition_by: ^partition_by, order_by: ^order_by])

      conditions = demands_to_conditions(demands, limiter)

      partitioned_query
      |> subquery()
      |> where(^conditions)
      |> order_by(^order_by)
      |> limit(^max(limit, 0))
      |> select([:id])
    end
  end

  # Partitioning Helpers

  defp partitioned?(%{global_demand: [_ | _]}), do: true
  defp partitioned?(%{rate_demand: [_ | _]}), do: true
  defp partitioned?(_changes), do: false

  defp partition_by_fields(partition) do
    case partition do
      %{fields: ["worker"]} ->
        [:worker]

      %{fields: ["args"], keys: []} ->
        [:args]

      %{fields: ["args"], keys: keys} ->
        for key <- keys, do: dynamic([j], fragment("?->>?", j.args, ^key))
    end
  end

  defp demands_to_conditions(demands, limiter) do
    base_allowed = dynamic([i], i.rank <= ^limiter.allowed)

    untracked_condition =
      Enum.reduce(demands, base_allowed, fn {_, worker, args}, acc ->
        case limiter.partition.fields do
          ["worker"] ->
            dynamic([i], i.worker != ^worker and ^acc)

          ["args"] ->
            if args == %{} do
              dynamic([i], not contained_in?(i.args, ^args) and ^acc)
            else
              dynamic([i], not contains?(i.args, ^args) and ^acc)
            end

          [_, _] ->
            if args == %{} do
              dynamic([i], i.worker != ^worker and not contained_in?(i.args, ^args) and ^acc)
            else
              dynamic([i], i.worker != ^worker and not contains?(i.args, ^args) and ^acc)
            end
        end
      end)

    demands
    |> Enum.reject(&(elem(&1, 0) == 0))
    |> Enum.reduce(untracked_condition, fn {allowed, worker, args}, acc ->
      case limiter.partition.fields do
        ["worker"] ->
          dynamic([i], (i.rank <= ^allowed and i.worker == ^worker) or ^acc)

        ["args"] ->
          if args == %{} do
            dynamic([i], (i.rank <= ^allowed and contained_in?(i.args, ^args)) or ^acc)
          else
            dynamic([i], (i.rank <= ^allowed and contains?(i.args, ^args)) or ^acc)
          end

        [_, _] ->
          if args == %{} do
            dynamic(
              [i],
              (i.rank <= ^allowed and i.worker == ^worker and contained_in?(i.args, ^args)) or
                ^acc
            )
          else
            dynamic(
              [i],
              (i.rank <= ^allowed and i.worker == ^worker and contains?(i.args, ^args)) or ^acc
            )
          end
      end
    end)
  end

  # Tracking Helpers

  defp track_jobs(_repo, %{jobs: [], producer: producer}) do
    {:ok, producer}
  end

  defp track_jobs(_repo, %{conf: conf, jobs: jobs, producer: producer}) do
    meta =
      producer.meta
      |> Rate.track(jobs)
      |> Global.track(jobs)

    query =
      Producer
      |> where([p], p.uuid == ^producer.uuid)
      |> select([p], p)
      |> update([p],
        set: [
          updated_at: ^utc_now(),
          meta: ^meta,
          running_ids: fragment("? || ?", p.running_ids, ^Enum.map(jobs, & &1.id))
        ]
      )

    {1, [new_producer]} = Repo.update_all(conf, query, [])

    {:ok, %{new_producer | refresh_interval: producer.refresh_interval}}
  end

  defp ack_job(_repo, %{conf: conf, job: job}) do
    producer = get_producer(conf, job)

    ack_job(conf, producer, job)
  end

  defp ack_job(conf, producer, job) do
    query =
      Producer
      |> where([p], p.uuid == ^producer.uuid)
      |> update([p], pull: [running_ids: ^job.id])

    query =
      case producer do
        %{meta: %{global_limit: %{partition: partition}}} ->
          {key, _worker, _args} = Global.job_to_key(job, partition)

          update(query, [p], set: [meta: dec_tracked_count(p.meta, ^key)])

        _ ->
          query
      end

    Repo.update_all(conf, query, [])

    {:ok, nil}
  end

  defp ack_cancelled_jobs(_repo, %{conf: conf, jobs: {_count, jobs}}) do
    producers =
      jobs
      |> Enum.group_by(& &1.queue)
      |> Map.new(fn {queue, [job | _]} -> {queue, get_producer(conf, job)} end)

    Enum.each(jobs, fn job ->
      case Map.fetch!(producers, job.queue) do
        %Producer{} = producer -> ack_job(conf, producer, job)
        _ -> :ok
      end
    end)

    {:ok, nil}
  end

  defp oper_opts(%{log: log, prefix: prefix}), do: [log: log, prefix: prefix]

  defp transaction(multi, conf, opts \\ []) do
    {:ok, _changes} = Repo.transaction(conf, multi, opts)
  end

  # Insert Helpers

  @uniq_lock_query """
  SELECT key FROM UNNEST($1::int[]) key WHERE NOT pg_try_advisory_xact_lock($2, key)
  """

  defp take_uniq_locks(_repo, %{uniques: []}), do: {:ok, MapSet.new()}

  defp take_uniq_locks(_repo, %{conf: conf, opts: opts, uniques: uniques}) do
    pref_key = :erlang.phash2(conf.prefix)
    lock_keys = Enum.map(uniques, &get_uniq_key/1)

    with {:ok, %{rows: rows}} <- Repo.query(conf, @uniq_lock_query, [lock_keys, pref_key], opts) do
      {:ok, MapSet.new(rows, &List.first/1)}
    end
  end

  defp find_uniq_dupes(_repo, %{uniques: []}), do: {:ok, MapSet.new()}

  defp find_uniq_dupes(_repo, %{conf: conf, opts: opts, uniques: uniques}) do
    empty_query = from j in Job, select: [0, 0], where: false

    dupes_query =
      Enum.reduce(uniques, empty_query, fn changeset, acc ->
        uniq_key = get_uniq_key(changeset)
        uniq_conditions = uniq_conditions(changeset)

        query =
          Job
          |> select([j], [^uniq_key, j.id])
          |> where(^uniq_conditions)

        union(acc, ^query)
      end)

    dupe_map =
      conf
      |> Repo.all(dupes_query, Keyword.put(opts, :prepare, :unnamed))
      |> Map.new(fn [key, id] -> {key, id} end)

    {:ok, dupe_map}
  end

  defp with_uniq_key(%{changes: %{unique: %{fields: _}}} = changeset) do
    uniq_key = Utils.to_uniq_key(changeset)

    meta =
      changeset
      |> Changeset.get_change(:meta, %{})
      |> Map.put(:uniq_key, uniq_key)

    Changeset.put_change(changeset, :meta, meta)
  end

  defp with_uniq_key(changeset), do: changeset

  defp take_keys(changeset, field, keys) do
    normalized =
      changeset
      |> Changeset.get_field(field)
      |> Map.new(fn {key, val} -> {to_string(key), val} end)

    if keys == [] do
      normalized
    else
      Map.take(normalized, Enum.map(keys, &to_string/1))
    end
  end

  defp get_uniq_key(changeset, default \\ nil) do
    get_in(changeset.changes, [:meta, :uniq_key]) || default
  end

  defp uniq_conditions(%{changes: %{unique: unique}} = changeset) do
    %{fields: fields, keys: keys, period: period, states: states} = unique

    query = dynamic([j], j.state in ^Enum.map(states, &to_string/1))
    query = Enum.reduce(fields, query, &unique_field({changeset, &1, keys}, &2))

    if period == :infinity do
      query
    else
      since = DateTime.add(utc_now(), period * -1, :second)

      dynamic([j], j.inserted_at >= ^since and ^query)
    end
  end

  defp unique_field({changeset, field, keys}, acc) when field in [:args, :meta] do
    value = take_keys(changeset, field, keys)

    if value == %{} do
      dynamic([j], fragment("? <@ ?", field(j, ^field), ^value) and ^acc)
    else
      dynamic([j], fragment("? @> ?", field(j, ^field), ^value) and ^acc)
    end
  end

  defp unique_field({changeset, field, _}, acc) do
    value = Changeset.get_field(changeset, field)

    dynamic([j], field(j, ^field) == ^value and ^acc)
  end

  defp insert_entries(_repo, %{changesets: []}), do: {:ok, []}

  defp insert_entries(_repo, %{conf: conf, changesets: changesets, opts: opts} = changes) do
    placeholders =
      changesets
      |> Enum.map(&Job.to_map/1)
      |> Enum.reduce(fn changeset, acc ->
        for {key, val} <- acc, changeset[key] == val, into: %{}, do: {key, val}
      end)

    entries =
      for changeset <- changesets, not_dupe?(changeset, changes) do
        changeset
        |> Job.to_map()
        |> Map.new(fn {key, val} ->
          if Map.has_key?(placeholders, key) do
            {key, {:placeholder, key}}
          else
            {key, val}
          end
        end)
      end

    opts = Keyword.merge(opts, on_conflict: :nothing, placeholders: placeholders, returning: true)

    {_count, jobs} = Repo.insert_all(conf, Job, entries, opts)

    {:ok, jobs}
  end

  defp not_dupe?(changeset, %{dupe_map: dupe_map, xact_set: xact_set}) do
    uniq_key = get_in(changeset.changes, [:meta, :uniq_key])

    is_nil(uniq_key) or
      (not Map.has_key?(dupe_map, uniq_key) and
         not MapSet.member?(xact_set, uniq_key))
  end

  defp apply_replacements(_repo, %{uniques: []}), do: {:ok, []}

  defp apply_replacements(_repo, %{conf: conf, dupe_map: dupe_map, opts: opts, uniques: uniques}) do
    updated =
      uniques
      |> Enum.filter(&get_in(&1.changes, [:replace]))
      |> Enum.reduce(%{}, fn %{changes: changes}, acc ->
        uniq_key = get_in(changes, [:meta, :uniq_key])
        job_id = Map.get(dupe_map, uniq_key)

        changes
        |> Map.take(changes.replace)
        |> Enum.reduce(acc, fn {key, val}, sub_acc ->
          Map.update(sub_acc, {key, val}, [job_id], &[job_id | &1])
        end)
      end)
      |> Enum.map(fn {val, ids} ->
        conf
        |> Repo.update_all(where(Job, [j], j.id in ^ids), [set: [val]], opts)
        |> elem(0)
      end)

    {:ok, Enum.sum(updated)}
  end

  defp apply_conflicts(_repo, %{new_jobs: jobs, uniques: []}), do: {:ok, jobs}

  defp apply_conflicts(_repo, %{dupe_map: dupe_map, new_jobs: [], uniques: uniques})
       when map_size(dupe_map) == 0 do
    dupes =
      Enum.map(uniques, fn changeset ->
        changeset
        |> Changeset.apply_action!(:insert)
        |> Map.replace!(:conflict?, true)
      end)

    {:ok, dupes}
  end

  defp apply_conflicts(_repo, %{dupe_map: dupe_map, new_jobs: jobs, uniques: uniques}) do
    lookup =
      Map.new(uniques, fn changeset ->
        {get_in(changeset.changes, [:meta, :uniq_key]), changeset}
      end)

    dupes =
      Enum.map(dupe_map, fn {uniq_key, job_id} ->
        lookup
        |> Map.fetch!(uniq_key)
        |> Changeset.apply_action!(:insert)
        |> Map.replace!(:id, job_id)
        |> Map.replace!(:conflict?, true)
      end)

    {:ok, jobs ++ dupes}
  end

  # Format Helpers

  defp seconds_from_now(seconds), do: DateTime.add(utc_now(), seconds, :second)

  defp format_blamed(%{unsaved_error: unsaved_error}) do
    %{kind: kind, reason: error, stacktrace: stacktrace} = unsaved_error

    {blamed, stacktrace} = Exception.blame(kind, error, stacktrace)

    Exception.format(kind, blamed, stacktrace)
  end

  # Stability

  defp jittery_sleep(base \\ 10, jitter \\ 0.5) do
    diff = base * jitter

    trunc(base - diff)..trunc(base + diff)
    |> Enum.random()
    |> Process.sleep()
  end

  defp with_retry(meta, fun, attempt \\ 0) do
    fun.()
  rescue
    error in [DBConnection.ConnectionError, Postgrex.Error] ->
      if attempt < meta.retry_attempts do
        jittery_sleep(attempt * meta.retry_backoff)

        with_retry(meta, fun, attempt + 1)
      else
        reraise error, __STACKTRACE__
      end
  end
end

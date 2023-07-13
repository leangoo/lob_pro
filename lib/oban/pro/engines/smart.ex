defmodule Oban.Pro.Engines.Smart do
  @moduledoc """
  The `Smart` engine provides advanced features such as truly global concurrency, global rate
  limiting, queue partitioning, unique bulk inserts, and auto insert batching. Additionally, its
  enhanced obvservability provides the foundation for accurate pruning with the
  Oban.Pro.Plugins.DynamicPruner and persistence for Oban.Pro.Plugins.DynamicQueues.

  ## Installation

  See the [Smart Engine](adoption.md#1-smart-engine) section in the [adoption guide](adoption.md)
  to get started. The [Producer Migrations](#producer-migrations) contains additional details for
  more complex setups with multiple instances or prefixes.

  ## Unique/Batched Insert All

  Where the `Basic` engine requires you to insert unique jobs individually, the `Smart` engine adds
  unique job support to `Oban.insert_all/2`. No additional configuration is necessary—simply use
  `insert_all` instead for unique jobs.

  ```elixir
  Oban.insert_all(lots_of_unique_jobs)
  ```

  Bulk insert also features automatic batching to support inserting an arbitrary number of jobs
  without hitting database limits (PostgreSQL's binary protocol has a limit of 65,535 parameters
  that may be sent in a single call. That presents an upper limit on the number of rows that may be
  inserted at one time.)

  ```elixir
  list_of_args
  |> Enum.map(&MyApp.MyWorker.new/1)
  |> Oban.insert_all()
  ```

  The default batch size for unique jobs is `250`, and `1_000` for non-unique jobs. Regardless, you
  can override with `batch_size`:

  ```elixir
  Oban.insert_all(lots_of_jobs, batch_size: 1500)
  ```

  It's also possible to set a custom timeout for batch inserts:

  ```elixir
  Oban.insert_all(lots_of_jobs, timeout: :infinity)
  ```

  A single batch of jobs is inserted without a transaction. Above that, each batch of jobs is
  inserted in a single transaction, _unless_ there are 10k total unique jobs to insert. After that
  threshold each batch is committed in a separate transaction to prevent memory errors. It's
  possible to control the transaction threshold with `xact_limit` if you happen to have a tuned
  database. For example, to set the limit at 20k jobs:

  ```elixir
  Oban.insert_all(lots_of_jobs, xact_limit: 20_000)
  ```

  ## Global Concurrency

  Global concurrency limits the number of concurrent jobs that run across all nodes.

  Typically the global concurrency limit is `local_limit * num_nodes`. For example, with three
  nodes and a local limit of 10, you'll have a global limit of 30. If a `global_limit` is present,
  and the `local_limit` is omitted, then the `local_limit` falls back to the `global_limit`.

  The only way to guarantee that all connected nodes will run _exactly one job_ concurrently is to
  set `global_limit: 1`.

  Here are some examples:

  ```elixir
  # Execute 10 jobs concurrently across all nodes, with up to 10 on a single node
  my_queue: [global_limit: 10]

  # Execute 10 jobs concurrently, but only 3 jobs on a single node
  my_queue: [local_limit: 3, global_limit: 10]

  # Execute at most 1 job concurrently
  my_queue: [global_limit: 1]
  ```

  ## Rate Limiting

  Rate limiting controls the number of jobs that execute within a period of time.

  Rate limiting uses counts for the same queue from all other nodes in the cluster (with or
  without Distributed Erlang). The limiter uses a sliding window over the configured period to
  accurately approximate a limit.

  Every job execution counts toward the rate limit, regardless of whether the job completes,
  errors, snoozes, etc.

  Without a modifier, the `rate_limit` period is defined in seconds. However, you can provide a
  `:second`, `:minute`, `:hour` or `:day` modifier to use more intuitive values.

  * `period: 30` — 30 seconds
  * `period: {1, :minute}` — 60 seconds
  * `period: {2, :minutes}` — 120 seconds
  * `period: {1, :hour}` — 3,600 seconds
  * `period: {1, :day}` —86,400 seconds

  Here are a few examples:

  ```elixir
  # Execute at most 1 job per second, the lowest possible limit
  my_queue: [rate_limit: [allowed: 1, period: 1]]

  # Execute at most 10 jobs per 30 seconds
  my_queue: [rate_limit: [allowed: 10, period: 30]]

  # Execute at most 10 jobs per minute
  my_queue: [rate_limit: [allowed: 10, period: {1, :minute}]]

  # Execute at most 1000 jobs per hour
  my_queue: [rate_limit: [allowed: 1000, period: {1, :hour}]]
  ```

  > #### Understanding Concurrency Limits {: .info}
  >
  > The local, global, or rate limit with the **lowest value** determines how many jobs are executed
  > concurrently. For example, with a `local_limit` of 10 and a `global_limit` of 20, a single node
  > will run 10 jobs concurrently. If that same queue had a `rate_limit` that allowed 5 jobs within
  > a period, then a single node is limited to 5 jobs.

  ## Queue Partitioning

  In addition to global and rate limits at the queue level, you can partition a queue so that it's
  treated as multiple queues where concurrency or rate limits apply separately to each partition.

  Partitions are specified with `fields` and `keys`, where `keys` is optional but _highly_
  recommended if you've included `:args`. Aside from making partitions easier to reason about,
  partitioning by keys minimizes the amount of data a queue needs to track and simplifies
  job-fetching queries.

  ### Configuring Partitions

  The partition syntax is identical for global and rate limits (note that you can partition by
  _global or rate_, but not both.)

  Here are a few examples of viable partitioning schemes:

  ```elixir
  # Partition by worker alone
  partition: [fields: [:worker]]

  # Partition by the `id` and `account_id` from args, ignoring the worker
  partition: [fields: [:args], keys: [:id, :account_id]]

  # Partition by worker and the `account_id` key from args
  partition: [fields: [:worker, :args], keys: [:account_id]]
  ```

  Remember, take care to minimize partition cardinality by using a few `keys` whenever possible.
  Partitioning based on _every permutation_ of your `args` makes concurrency or rate limits hard to
  reason about and can negatively impact queue performance.

  ### Global Partitioning

  Global partitioning changes global concurency behavior. Rather than applying a fixed number for
  the queue, it applies to every partition within the queue.

  Consider the following example:

  ```elixir
  global_limit: [allowed: 1, partition: [fields: [:worker]]]
  ```

  The queue is configured to run one job per-worker across every node, but only 10 concurrently on a
  single node. That is in contrast to the standard behaviour of `global_limit`, which would override
  the `local_limit` and only allow 1 concurrent job across every node.

  Alternatively, you could partition by a single key:

  ```elixir
  global_limit: [allowed: 1, partition: [fields: [:args], keys: [:tenant_id]]]
  ```

  That configures the queue to run one job concurrently across the entire cluster per `tenant_id`.

  ### Rate Limit Partitioning

  Rate limit partitions operate similarly to global partitions. Rather than limiting all jobs within
  the queue, they limit each partition within the queue.

  For example, to allow one job per-worker, every five seconds, across every instance of the `alpha`
  queue in your cluster:

  ```elixir
  local_limit: 10, rate_limit: [allowed: 1, period: 5, partition: [fields: [:worker]]]
  ```

  ## Producer Migrations

  For multiple Oban instances you'll need to configure each one to use the `Smart` engine,
  otherwise they'll default to the `Basic` engine.

  If you use prefixes, or have multiple instances with different prefixes, you can specify the
  prefix and create multiple tables in one migration:

  ```elixir
  use Ecto.Migration

  def change do
  Oban.Pro.Migrations.Producers.change()
  Oban.Pro.Migrations.Producers.change(prefix: "special")
  Oban.Pro.Migrations.Producers.change(prefix: "private")
  end
  ```

  The `Producers` migration also exposes `up/0` and `down/0` functions if `change/0` doesn't fit
  your usecase.
  """

  @behaviour Oban.Engine

  import Ecto.Query
  import DateTime, only: [utc_now: 0]

  alias Ecto.{Changeset, Multi}
  alias Oban.{Backoff, Config, Engine, Job, Repo}
  alias Oban.Engines.Basic
  alias Oban.Pro.Limiters.{Global, Rate}
  alias Oban.Pro.{Producer, Utils}

  @type partition :: [fields: [:args | :worker], keys: [atom()]]

  @type period :: pos_integer() | {pos_integer(), unit()}

  @type global_limit :: pos_integer() | [allowed: pos_integer(), partition: partition()]

  @type local_limit :: pos_integer()

  @type rate_limit :: [allowed: pos_integer(), period: period(), partition: partition()]

  @type unit :: :second | :seconds | :minute | :minutes | :hour | :hours | :day | :days

  @base_batch_size 1_000
  @base_xact_limit 10_000
  @uniq_batch_size 250

  defmacrop dec_tracked_count(meta, key) do
    quote do
      fragment(
        """
        CASE (? #> ?)::text::int
        WHEN 1 THEN ? #- ?
        ELSE jsonb_set(?, ?, ((? #> ?)::text::int - 1)::text::jsonb)
        END
        """,
        unquote(meta),
        ["global_limit", "tracked", unquote(key), "count"],
        unquote(meta),
        ["global_limit", "tracked", unquote(key)],
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
    # Recording the paused state is best-effort only. There's no reason to crash at this point
    # during shutdown.
    _kind, _reason ->
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
  def check_meta(_conf, %Producer{} = producer, running) do
    jids = for {_, {_, exec}} <- running, do: exec.job.id

    meta =
      producer.meta
      |> Map.from_struct()
      |> flatten_windows()

    producer
    |> Map.take([:name, :node, :queue, :started_at, :updated_at])
    |> Map.put(:running, jids)
    |> Map.merge(meta)
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
      case Process.delete(:oban_meta) do
        nil -> set
        meta -> Keyword.put(set, :meta, meta)
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
      push: [errors: Job.format_attempt(job)]
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

    updates = [set: set, push: [errors: Job.format_attempt(job)]]

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
    scoped = where(Job, id: ^job.id)

    {subquery, updates} =
      if is_map(job.unsaved_error) do
        updates = [
          set: [state: "cancelled", cancelled_at: utc_now()],
          push: [errors: Job.format_attempt(job)]
        ]

        {scoped, updates}
      else
        updates = [state: "cancelled", cancelled_at: utc_now()]

        {where(scoped, [j], j.state not in ^~w(cancelled completed discarded)), updates}
      end

    query =
      Job
      |> join(:inner, [j], x in subquery(subquery), on: j.id == x.id)
      |> select([_, x], map(x, [:id, :args, :attempted_by, :queue, :state, :worker]))

    Multi.new()
    |> Multi.put(:conf, conf)
    |> Multi.update_all(:jobs, query, updates, oper_opts(conf))
    |> Multi.run(:ack, &ack_cancelled_jobs/2)
    |> transaction(conf)

    :ok
  end

  @impl Engine
  def cancel_all_jobs(%Config{} = conf, queryable) do
    subquery = where(queryable, [j], j.state not in ["cancelled", "completed", "discarded"])

    query =
      Job
      |> join(:inner, [j], x in subquery(subquery), on: j.id == x.id)
      |> update(set: [state: "cancelled", cancelled_at: ^utc_now()])
      |> select([_, x], map(x, [:id, :args, :attempted_by, :queue, :state, :worker]))

    {:ok, %{jobs: {_count, jobs}}} =
      Multi.new()
      |> Multi.put(:conf, conf)
      |> Multi.update_all(:jobs, query, [], oper_opts(conf))
      |> Multi.run(:ack, &ack_cancelled_jobs/2)
      |> transaction(conf)

    {:ok, jobs}
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
  def insert_all_jobs(conf, changesets, opts) do
    xact_limit = Keyword.get(opts, :xact_limit, @base_xact_limit)
    batch_size = Keyword.get(opts, :batch_size, @base_batch_size)
    uniq_count = Enum.count(changesets, &unique?/1)

    cond do
      uniq_count == 0 and length(changesets) <= batch_size ->
        {:ok, jobs} = insert_entries(nil, %{conf: conf, changesets: changesets, opts: opts})

        jobs

      uniq_count < xact_limit ->
        inner_insert_all(conf, changesets, opts)

      true ->
        fun = fn -> inner_insert_all(conf, changesets, opts) end

        {:ok, jobs} = Repo.transaction(conf, fun, opts)

        jobs
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
  defdelegate retry_job(conf, job), to: Basic

  @impl Engine
  defdelegate retry_all_jobs(conf, queryable), to: Basic

  # Producer Fetching Helpers

  defp put_producer(conf, producer) do
    Registry.update_value(Oban.Registry, reg_key(conf, producer), fn _ -> producer end)
  end

  defp get_producer(conf, job) do
    with :error <- get_producer_from_registry(conf, job),
         :error <- get_producer_from_database(conf, job),
         [_node, uuid] <- job.attempted_by do
      %Producer{uuid: uuid}
    end
  end

  defp get_producer_from_registry(conf, job) do
    case Oban.Registry.lookup(reg_key(conf, job)) do
      {_pid, producer} -> producer
      nil -> :error
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

  defp reg_key(%{name: name}, %{queue: queue}) do
    {name, {:producer, queue}}
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
    subset_query = fetch_subquery(changes)

    query =
      Job
      |> with_cte("subset", as: ^subset_query)
      |> join(:inner, [j], x in fragment(~s("subset")), on: true)
      |> where([j, x], j.id == x.id)
      |> where([j, _], j.attempt < j.max_attempts)
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

    {_count, jobs} = Repo.update_all(conf, query, updates, options)

    {:ok, jobs}
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
    |> select([:id])
    |> order_by(asc: :priority, asc: :scheduled_at, asc: :id)
    |> limit(^max(limit, 0))
    |> lock("FOR UPDATE SKIP LOCKED")
  end

  defp fetch_subquery(producer, limiter, limit, demands) do
    if Enum.all?(demands, &(elem(&1, 0) >= limit)) do
      fetch_subquery(producer, limit)
    else
      partition = limiter.partition
      partition_by = partition_by_fields(partition)
      order_by = [asc: :priority, asc: :scheduled_at, asc: :id]

      partitioned_query =
        Job
        |> select([j], %{id: j.id, priority: j.priority, scheduled_at: j.scheduled_at})
        |> select_merge([j], %{worker: j.worker, args: j.args})
        |> select_merge([j], %{rank: over(dense_rank(), :partition)})
        |> windows([j], partition: [partition_by: ^partition_by, order_by: ^order_by])
        |> where(state: "available", queue: ^producer.queue)
        |> order_by(^order_by)
        |> limit(^max(limit * 10, 100))

      conditions = demands_to_conditions(demands, limiter)

      partitioned_query
      |> subquery()
      |> select([:id])
      |> where(^conditions)
      |> order_by(^order_by)
      |> limit(^max(limit, 0))
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
    %{refresh_interval: interval, uuid: uuid} = producer

    meta =
      producer.meta
      |> Rate.track(jobs)
      |> Global.track(jobs)

    query =
      Producer
      |> where([p], p.uuid == ^uuid)
      |> select([p], p)
      |> update([p], set: [updated_at: ^utc_now(), meta: ^meta])

    case Repo.update_all(conf, query, []) do
      {1, [producer]} ->
        {:ok, %{producer | refresh_interval: interval}}

      {0, _} ->
        # In this case the producer was erroneously deleted, possibly due to a connection error,
        # downtime, or in development after waking from sleep.
        %{producer | meta: meta, updated_at: utc_now()}
        |> Changeset.change()
        |> then(&Repo.insert(conf, &1))
    end
  end

  defp ack_job(_repo, %{conf: conf, job: job}) do
    producer = get_producer(conf, job)

    ack_job(conf, producer, job)
  end

  defp ack_job(conf, producer, job) do
    with %{meta: %{global_limit: %{partition: partition}}} <- producer do
      {key, _worker, _args} = Global.job_to_key(job, partition)

      Producer
      |> where([p], p.uuid == ^producer.uuid)
      |> update([p], set: [meta: dec_tracked_count(p.meta, ^key)])
      |> then(&Repo.update_all(conf, &1, []))
    end

    {:ok, nil}
  end

  defp ack_cancelled_jobs(_repo, %{conf: conf, jobs: {_count, jobs}}) do
    jobs
    |> Enum.filter(&(&1.state == "executing"))
    |> Enum.group_by(& &1.queue)
    |> Enum.map(fn {_queue, [job | _] = jobs} -> {get_producer(conf, job), jobs} end)
    |> Enum.each(fn {producer, jobs} ->
      for job <- jobs, do: ack_job(conf, producer, job)
    end)

    {:ok, nil}
  end

  defp oper_opts(conf), do: Repo.default_options(conf)

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
    empty_query = from j in Job, select: [0, 0, "available"], where: false

    dupes_query =
      Enum.reduce(uniques, empty_query, fn changeset, acc ->
        uniq_key = get_uniq_key(changeset)
        uniq_conditions = uniq_conditions(changeset)

        query =
          Job
          |> select([j], [^uniq_key, j.id, j.state])
          |> where(^uniq_conditions)

        union(acc, ^query)
      end)

    dupe_map =
      conf
      |> Repo.all(dupes_query, Keyword.put(opts, :prepare, :unnamed))
      |> Map.new(fn [key, id, state] -> {key, {id, state}} end)

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

  defp not_dupe?(_changeset, _changes), do: true

  defp apply_replacements(_repo, %{uniques: []}), do: {:ok, []}

  defp apply_replacements(_repo, %{conf: conf, dupe_map: dupe_map, opts: opts, uniques: uniques}) do
    updates =
      for %{changes: %{replace: replace} = changes} <- uniques,
          is_map_key(dupe_map, changes.meta.uniq_key),
          reduce: %{} do
        acc ->
          {job_id, job_state} = Map.get(dupe_map, changes.meta.uniq_key)

          job_state = String.to_existing_atom(job_state)
          rep_keys = Keyword.get(replace, job_state, [])

          changes
          |> Map.take(rep_keys)
          |> Enum.reduce(acc, fn {key, val}, sub_acc ->
            Map.update(sub_acc, {key, val}, [job_id], &[job_id | &1])
          end)
      end

    Enum.each(updates, fn {val, ids} ->
      conf
      |> Repo.update_all(where(Job, [j], j.id in ^ids), [set: [val]], opts)
      |> elem(0)
    end)

    {:ok, []}
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
      Enum.map(dupe_map, fn {uniq_key, {job_id, _job_state}} ->
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

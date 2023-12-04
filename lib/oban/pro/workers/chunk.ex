defmodule Oban.Pro.Workers.Chunk do
  @moduledoc """
  Chunk workers execute jobs together in groups based on a size or a timeout option. e.g. when
  1000 jobs are available or after 10 minutes have ellapsed.

  Multiple chunks can run in parallel within a single queue, and each chunk may be composed of
  many thousands of jobs. Aside from improved throughput for a single queue, chunks are ideal as
  the initial stage of data-ingestion and data-processing pipelines.

  ## Usage

  Let's define a worker that sends SMS messages in chunks, rather than individually:

  ```elixir
  defmodule MyApp.ChunkWorker do
    use Oban.Pro.Workers.Chunk, queue: :messages, size: 100, timeout: 1000

    @impl true
    def process([_ | _] = jobs) do
      jobs
      |> Enum.map(& &1.args)
      |> MyApp.Messaging.send_batch()

      :ok
    end
  end
  ```

  Notice that we declared a `size` and a `timeout` along with a `queue` for the worker. If `size`
  or `timeout` are omitted they fall back to their defaults: 100 `size` and 1000ms respectively.

  To process _larger_ batches _less_ frequently, we can increase both values:

  ```elixir
  use Oban.Pro.Workers.Chunk, size: 500, timeout: :timer.seconds(5)
  ```

  Now chunks will run with up to 500 jobs or every 5 seconds, whichever comes first.

  Unlike other Pro workers, a `Chunk` worker's `process/1` receives a list of jobs rather than a
  single job struct. Fittingly, the [expected return values](#t:result/0) are different as well.

  ## Chunk Partitioning

  By default, chunks are divided into groups based on the `queue` and `worker`. This means that
  each chunk consists of workers belonging to the same queue, regardless of their `args` or
  `meta`. However, this approach may not always be suitable. For instance, you may want to group
  workers based on a specific argument such as `:account_id` instead of just the worker. In such
  cases, you can use the [`:by`](#t:chunk_by/0) option to customize how chunks are partitioned.

  Here are a few examples of the `:by` option that you can use to achieve fine-grained control over
  chunk partitioning:

  ```elixir
  # Explicitly chunk by :worker
  use Oban.Pro.Workers.Chunk, by: :worker

  # Chunk by a single args key without considering the worker
  use Oban.Pro.Workers.Chunk, by: [args: :account_id]

  # Chunk by multiple args keys without considering the worker
  use Oban.Pro.Workers.Chunk, by: [args: [:account_id, :cohort]]

  # Chunk by worker and a single args key
  use Oban.Pro.Workers.Chunk, by: [:worker, args: :account_id]

  # Chunk by a single meta key
  use Oban.Pro.Workers.Chunk, by: [meta: :source_id]
  ```

  When partitioning chunks of jobs, it's important to note that using only `:args` or `:meta`
  without `:worker` may result in heterogeneous chunks of jobs from different workers.
  Nevertheless, regardless of the partitioning method chunks always consist of jobs from the same
  queue.

  Here's a simple example of partitioning by `worker` and an `author_id` field to batch analysis
  of recent messages per author:

  ```elixir
  defmodule MyApp.LLMAnalysis do
    use Oban.Pro.Workers.Chunk, by: [:worker, args: :author_id], size: 50, timeout: 30_000

    @impl true
    def process([%{args: %{"author_id" => author_id}} | _] = jobs) do
      messages =
        jobs
        |> Enum.map(& &1.args["message_id"])
        |> MyApp.Messages.all()

      {:ok, sentiment} = MyApp.GPT.gauge_sentiment(messages)

      MyApp.Messages.record_sentiment(author_id)
    end
  end
  ```

  ## Chunk Results and Error Handling

  `Chunk` worker's result type is tailored to handling multiple jobs at once. For reference, here
  are the types and callback definition for `process/1`:

  The result types allow you to succeed an entire chunk, or selectively fail parts of it. Here are
  each of the possible results:

  * `:ok` — The chunk succeeded and all jobs can be marked complete

  * `{:ok, result}` — Like `:ok`, but with a result for testing.

  * `{:error, reason, jobs}` — One or more jobs in the chunk failed and may be retried, any
    unlisted jobs are a success.

  * `{:cancel, reason, jobs}` — One or more jobs in the chunk should be cancelled, any unlisted
    jobs are a success.

  * `[error: {reason, jobs}, cancel: {reason, jobs}]` — Retry some jobs and cancel some other
    jobs, leaving any jobs not in either list a success.

  To illustrate using chunk results let's expand on the message processing example from earlier.
  We'll extend it to complete the whole batch when all messages are delivered or cancel
  undeliverable messages:

  ```elixir
  def process([_ | _] = jobs) do
    notifications =
      jobs
      |> Enum.map(& &1.args)
      |> MyApp.Messaging.send_batch()

    bad_token = fn %{response: response} -> response == :bad_token end

    if Enum.any?(notifications, bad_token) do
      cancels =
        notifications
        |> Enum.zip(jobs)
        |> Enum.filter(fn {notification, _job} -> bad_token.(notification) end)
        |> Enum.map(&elem(&1, 1))

      {:cancel, :bad_token, cancels}
    else
      {:ok, notifications}
    end
  end
  ```

  In the event of an ephemeral crash, like a network issue, the entire batch may be retried if
  there are any remaining attempts.

  Cancelling any jobs in a chunk will cancel the _entire_ chunk, including the leader job.

  ## Chunk Organization

  Chunks are ran by a leader job (which has nothing to do with peer leadership). When the leader
  executes it determines whether a complete chunk is available or if enough time has elapsed to
  run anyhow. If neither case applies, then the leader will delay until the timeout elapsed and
  execute with as many jobs as it can find.

  Completion queries run every `1000ms` by default. You can use the `:sleep` option to control how
  long the leader delays between queries to check for complete chunks:

  ```elixir
  use Oban.Pro.Workers.Chunk, size: 50, sleep: 2_000, timeout: 10_000
  ```

  ### Optimizing Chunks

  Queue's with high concurrency and low throughput may have multiple chunk leaders running
  simultaneously rather than getting bundled into a single chunk. The solution is to reduce the
  queue’s global concurrency to a smaller number so that new chunk leader jobs don’t start and the
  existing chunk can run a bigger batch.
  """

  import Ecto.Query
  import DateTime, only: [utc_now: 0]

  alias Ecto.Multi
  alias Oban.{CrashError, Job, Notifier, PerformError, Repo, Validation}
  alias Oban.Pro.Utils

  @type key_or_keys :: atom() | [atom()]

  @type chunk_by ::
          :worker
          | {:args, key_or_keys()}
          | {:meta, key_or_keys()}
          | [:worker | {:args, key_or_keys()} | {:meta, key_or_keys()}]

  @type sub_result :: {reason :: term(), [Job.t()]}

  @type result ::
          :ok
          | {:ok, term()}
          | {:cancel | :discard | :error, reason :: term(), [Job.t()]}
          | [cancel: sub_result(), discard: sub_result(), error: sub_result()]

  @type options :: [
          by: chunk_by(),
          size: pos_integer(),
          sleep: pos_integer(),
          timeout: pos_integer()
        ]

  # Purely used for validation
  defstruct [:by, :size, :sleep, :timeout]

  @doc false
  defmacro __using__(opts) do
    {chunk_opts, other_opts} = Keyword.split(opts, [:by, :size, :sleep, :timeout])

    quote do
      Validation.validate!(unquote(chunk_opts), &Oban.Pro.Workers.Chunk.validate/1)

      use Oban.Pro.Worker, unquote(other_opts)

      alias Oban.Pro.Workers.Chunk

      @default_meta %{
        chunk_by: Keyword.get(unquote(chunk_opts), :by, :worker),
        chunk_size: Keyword.get(unquote(chunk_opts), :size, 100),
        chunk_sleep: Keyword.get(unquote(chunk_opts), :sleep, 1000),
        chunk_timeout: Keyword.get(unquote(chunk_opts), :timeout, 1000)
      }

      @impl Oban.Worker
      def new(args, opts) when is_map(args) and is_list(opts) do
        opts =
          opts
          |> Keyword.update(:meta, @default_meta, &Map.merge(@default_meta, &1))
          |> update_in([:meta, :chunk_by], &Utils.normalize_by/1)

        super(args, opts)
      end

      @impl Oban.Worker
      def perform(%Job{} = job) do
        opts = __opts__()

        with {:ok, job} <- Oban.Pro.Worker.before_process(job, opts) do
          job
          |> Chunk.maybe_process(__MODULE__)
          |> Oban.Pro.Worker.after_process(job, opts)
        end
      end
    end
  end

  @doc false
  def validate(opts) do
    Validation.validate(opts, fn
      {:by, by} -> Oban.Pro.Validation.validate_by(by)
      {:size, size} -> Validation.validate_integer(:size, size)
      {:sleep, sleep} -> Validation.validate_timeout(:sleep, sleep)
      {:timeout, timeout} -> Validation.validate_timeout(:timeout, timeout)
      option -> {:unknown, option, __MODULE__}
    end)
  end

  # Public Interface

  @doc false
  def maybe_process(%Job{conf: conf} = job, worker) do
    last_ts = fetch_last_ts(conf, job)

    cond do
      full_timeout?(last_ts, job) ->
        fetch_and_process(conf, worker, job)

      full_chunk?(conf, job) ->
        fetch_and_process(conf, worker, job)

      true ->
        last_ts
        |> calculate_timeout(job)
        |> sleep_and_process(job, worker)
    end
  end

  # Check Helpers

  defp fetch_last_ts(conf, job) do
    query =
      Job
      |> where([j], j.id != ^job.id)
      |> where([j], j.state != "scheduled")
      |> chunk_where(job)
      |> order_by(desc: :id)
      |> limit(1)
      |> select(
        [j],
        type(
          fragment(
            "greatest(?, ?, ?, ?)",
            j.attempted_at,
            j.completed_at,
            j.discarded_at,
            j.cancelled_at
          ),
          :utc_datetime_usec
        )
      )

    Repo.one(conf, query)
  end

  defp full_chunk?(conf, %{meta: %{"chunk_size" => size}} = job) do
    limit = size - 1

    query =
      Job
      |> select([j], j.id)
      |> where([j], j.state == "available")
      |> chunk_where(job)
      |> limit(^limit)

    Repo.one(conf, from(oj in subquery(query), select: count(oj.id) >= ^limit))
  end

  defp full_timeout?(nil, _job), do: false

  defp full_timeout?(last_ts, %{meta: %{"chunk_timeout" => timeout}}) do
    comp =
      utc_now()
      |> DateTime.add(-timeout, :millisecond)
      |> DateTime.compare(last_ts)

    comp == :gt
  end

  defp calculate_timeout(nil, %{meta: %{"chunk_timeout" => timeout}}), do: timeout

  defp calculate_timeout(last_ts, %{meta: %{"chunk_timeout" => timeout}}) do
    max(0, timeout - DateTime.diff(utc_now(), last_ts, :millisecond))
  end

  # Processing Helpers

  defp fetch_and_process(conf, worker, %{meta: %{"chunk_size" => size}} = job) do
    {:ok, chunk} = fetch_chunk(conf, job, size - 1)
    {chunk, _errored} = prepare_chunk(chunk)

    guard_cancel(conf, Oban.whereis(conf.name), worker, job, chunk)
    guard_timeout(conf, worker.timeout(job), worker, job, chunk)

    try do
      case worker.process([job | chunk]) do
        :ok ->
          ack_completed(conf, chunk, :ok)

        {:ok, result} ->
          ack_completed(conf, chunk, {:ok, result})

        {:cancel, reason, cancelled} ->
          ack_cancelled(conf, worker, job, chunk, cancelled, reason)

        {:discard, reason, discarded} ->
          ack_discarded(conf, worker, job, chunk, discarded, reason)

        {:error, reason, errored} ->
          ack_errored(conf, worker, job, chunk, errored, reason)

        [_ | _] = multiple ->
          ack_multiple(conf, worker, job, chunk, multiple)
      end
    rescue
      error ->
        ack_raised(conf, worker, chunk, job, error, __STACKTRACE__)

        reraise error, __STACKTRACE__
    catch
      kind, reason ->
        error = CrashError.exception({kind, reason, __STACKTRACE__})

        ack_raised(conf, worker, chunk, job, error, __STACKTRACE__)

        reraise error, __STACKTRACE__
    end
  end

  # This replicates the query used in Smart.fetch_jobs/3, without the meta tracking or any other
  # complications. Any modifications to the original query must be replicated here. Another
  defp fetch_chunk(conf, job, limit) do
    subset_query =
      Job
      |> select([:id])
      |> where(state: "available")
      |> chunk_where(job)
      |> order_by(asc: :priority, asc: :scheduled_at, asc: :id)
      |> limit(^limit)
      |> lock("FOR UPDATE SKIP LOCKED")

    query =
      Job
      |> with_cte("subset", as: ^subset_query)
      |> join(:inner, [j], x in fragment(~s("subset")), on: true)
      |> where([j, x], j.id == x.id)
      |> select([j, _], j)

    attempted_by = job.attempted_by ++ ["chunk-#{job.id}"]

    updates = [
      set: [state: "executing", attempted_at: utc_now(), attempted_by: attempted_by],
      inc: [attempt: 1]
    ]

    Repo.transaction(conf, fn ->
      {_count, chunk} = Repo.update_all(conf, query, updates)

      chunk
    end)
  end

  defp prepare_chunk(chunk) do
    Enum.reduce(chunk, {[], []}, fn job, {acc, err} ->
      with {:ok, worker} <- Oban.Worker.from_string(job.worker),
           {:ok, job} <- Oban.Pro.Worker.before_process(job, worker.__opts__()) do
        {[job | acc], err}
      else
        {:error, reason} ->
          {acc, [{job, reason} | err]}
      end
    end)
  end

  # Only the leader job is registered as "running" by the queue producer. We listen for pkill
  # messages for _other_ jobs in the chunk and apply that to all jobs. Without this, cancelling
  # doesn't apply to the leader and chunk jobs are orphaned.
  #
  # During tests there may not be an Oban instance and we can ignore cancelling.
  defp guard_cancel(_conf, nil, _worker, _job, _chunk), do: :ok

  defp guard_cancel(conf, pid, worker, job, chunk) when is_pid(pid) do
    parent = self()

    Task.start(fn ->
      ref = Process.monitor(parent)
      :ok = Notifier.listen(conf.name, [:signal])

      await_cancel(conf, ref, worker, job, chunk)
    end)
  end

  defp await_cancel(conf, ref, worker, job, chunk) do
    receive do
      {:notification, :signal, %{"action" => "pkill", "job_id" => kill_id}} ->
        if Enum.any?(chunk, &(&1.id == kill_id)) do
          reason = PerformError.exception({job.worker, {:cancel, :shutdown}})

          ack_cancelled(conf, worker, job, chunk, chunk, reason)

          Notifier.notify(conf.name, :signal, %{action: "pkill", job_id: job.id})
        else
          await_cancel(conf, ref, worker, job, chunk)
        end

      {:DOWN, ^ref, :process, _pid, _reason} ->
        :ok
    end
  end

  # The executor uses :timer.exit_after/2 to kill jobs that exceed the timeout. The queue's
  # producer then catches the DOWN message and uses that to record a job error. The producer
  # isn't aware of the job's chunk, so we monitor the parent process and ack the chunk jobs.
  defp guard_timeout(_conf, :infinity, _worker, _job, _chunk), do: :ok

  defp guard_timeout(conf, timeout, worker, job, chunk) do
    parent = self()

    Task.start(fn ->
      ref = Process.monitor(parent)

      receive do
        {:DOWN, ^ref, :process, _pid, %Oban.TimeoutError{} = reason} ->
          ack_errored(conf, worker, job, chunk, chunk, reason)

        {:DOWN, ^ref, :process, _pid, _reason} ->
          :ok
      after
        timeout + 100 -> :ok
      end
    end)
  end

  defp sleep_and_process(timeout, %Job{conf: conf, meta: meta} = job, worker) do
    sleep = Map.get(meta, "chunk_sleep", 1000)

    Process.sleep(sleep)

    cond do
      full_chunk?(job.conf, job) ->
        fetch_and_process(conf, worker, job)

      timeout - sleep < 0 ->
        fetch_and_process(conf, worker, job)

      true ->
        sleep_and_process(timeout - sleep, job, worker)
    end
  end

  # Chunk Helpers

  defp chunk_where(query, job) do
    query = where(query, queue: ^job.queue)

    job.meta
    |> Map.get("chunk_by")
    |> Enum.reduce(query, fn
      "worker", acc ->
        where(acc, worker: ^job.worker)

      ["args", keys], acc ->
        where(acc, [j], fragment("? @> ?", j.args, ^take_keys(job.args, keys)))

      ["meta", keys], acc ->
        where(acc, [j], fragment("? @> ?", j.meta, ^take_keys(job.meta, keys)))
    end)
  end

  defp take_keys(args, keys) when is_struct(args) do
    Map.take(args, Enum.map(keys, &String.to_existing_atom/1))
  end

  defp take_keys(map, keys), do: Map.take(map, keys)

  # Ack Helpers

  defp ack_completed(conf, jobs, result) do
    Repo.update_all(conf, ids_query(jobs), com_ups())

    result
  end

  defp ack_raised(conf, worker, chunk, job, error, stacktrace) do
    {dis, ers} = Enum.split_with(chunk, &exhausted?/1)

    opts = Repo.default_options(conf)

    multi =
      Multi.new()
      |> Multi.update_all(:err, ids_query(ers), err_ups(worker, job, error, stacktrace), opts)
      |> Multi.update_all(:dis, ids_query(dis), dis_ups(worker, job, error, stacktrace), opts)

    Repo.transaction(conf, multi)

    :ok
  end

  defp ack_errored(conf, worker, job, chunk, errored, reason) do
    {ers, oks, set} = split_with_set(errored, chunk)
    {dis, ers} = Enum.split_with(ers, &exhausted?/1)

    opts = Repo.default_options(conf)

    multi =
      Multi.new()
      |> Multi.update_all(:com, ids_query(oks), com_ups(), opts)
      |> Multi.update_all(:err, ids_query(ers), err_ups(worker, job, {:error, reason}), opts)
      |> Multi.update_all(:dis, ids_query(dis), dis_ups(worker, job, {:error, reason}), opts)

    with {:ok, _result} <- Repo.transaction(conf, multi) do
      if MapSet.member?(set, job.id), do: {:error, reason}, else: :ok
    end
  end

  defp ack_cancelled(conf, worker, job, chunk, cancelled, reason) do
    {can, oks, set} = split_with_set(cancelled, chunk)

    opts = Repo.default_options(conf)

    multi =
      Multi.new()
      |> Multi.update_all(:com, ids_query(oks), com_ups(), opts)
      |> Multi.update_all(:can, ids_query(can), can_ups(worker, job, {:cancel, reason}), opts)

    with {:ok, _result} <- Repo.transaction(conf, multi) do
      if MapSet.member?(set, job.id), do: {:cancel, reason}, else: :ok
    end
  end

  defp ack_discarded(conf, worker, job, chunk, discarded, reason) do
    {dis, oks, set} = split_with_set(discarded, chunk)

    opts = Repo.default_options(conf)

    multi =
      Multi.new()
      |> Multi.update_all(:com, ids_query(oks), com_ups(), opts)
      |> Multi.update_all(:dis, ids_query(dis), dis_ups(worker, job, {:discard, reason}), opts)

    with {:ok, _result} <- Repo.transaction(conf, multi) do
      if MapSet.member?(set, job.id), do: {:discard, reason}, else: :ok
    end
  end

  defp ack_multiple(conf, worker, host, chunk, multiple) do
    {oks, result} = split_multiple_result(host, chunk, multiple)

    opts = Repo.default_options(conf)

    group = fn {oper, reason, %{attempt: attempt}} -> {oper, reason, attempt} end
    multi = Multi.update_all(Multi.new(), :com, ids_query(oks), com_ups(), opts)

    multi =
      for({oper, {reas, jobs}} <- multiple, job <- jobs, job.id != host.id, do: {oper, reas, job})
      |> Enum.map(&maybe_discard/1)
      |> Enum.group_by(group, &elem(&1, 2))
      |> Enum.with_index()
      |> Enum.reduce(multi, fn {{{oper, reason, _}, jobs}, index}, acc ->
        update =
          case oper do
            :cancel -> can_ups(worker, hd(jobs), {:cancel, reason})
            :discard -> dis_ups(worker, hd(jobs), {:discard, reason})
            :error -> err_ups(worker, hd(jobs), {:error, reason})
          end

        Multi.update_all(acc, {oper, index}, ids_query(jobs), update, opts)
      end)

    with {:ok, _} <- Repo.transaction(conf, multi), do: result
  end

  # Single Helpers

  defp ids_query(jobs) do
    where(Job, [j], j.id in ^Enum.map(jobs, & &1.id))
  end

  defp split_with_set(sub_jobs, all_jobs) do
    set = MapSet.new(sub_jobs, & &1.id)

    all_jobs
    |> Enum.split_with(&MapSet.member?(set, &1.id))
    |> Tuple.append(set)
  end

  defp exhausted?(%Job{attempt: attempt, max_attempts: max}), do: attempt >= max

  # Multiple Helpers

  defp split_multiple_result(%{id: jid}, chunk, multiple) do
    set = for {_, {_, jobs}} <- multiple, %{id: id} <- jobs, into: MapSet.new(), do: id

    result =
      if MapSet.member?(set, jid) do
        {oper, {reason, _}} =
          Enum.find(multiple, fn {_, {_, jobs}} -> jid in Enum.map(jobs, & &1.id) end)

        {oper, reason}
      else
        :ok
      end

    {Enum.reject(chunk, &MapSet.member?(set, &1.id)), result}
  end

  defp maybe_discard({:error, reason, job}) when job.attempt >= job.max_attempts do
    {:discard, reason, job}
  end

  defp maybe_discard(tuple), do: tuple

  # Update Helpers

  defp com_ups do
    [set: [state: "completed", completed_at: utc_now()]]
  end

  defp can_ups(worker, job, reason) do
    error = format_error(job, worker, reason, [])

    Keyword.new()
    |> Keyword.put(:set, state: "cancelled", cancelled_at: utc_now())
    |> Keyword.put(:push, errors: error)
  end

  defp err_ups(worker, job, error, stacktrace \\ []) do
    error = format_error(job, worker, error, stacktrace)

    Keyword.new()
    |> Keyword.put(:set, state: "retryable", scheduled_at: backoff_at(worker, job))
    |> Keyword.put(:push, errors: error)
  end

  defp dis_ups(worker, job, error, stacktrace \\ []) do
    error = format_error(job, worker, error, stacktrace)

    Keyword.new()
    |> Keyword.put(:set, state: "discarded", discarded_at: utc_now())
    |> Keyword.put(:push, errors: error)
  end

  defp backoff_at(worker, job) do
    DateTime.add(utc_now(), worker.backoff(job), :second)
  end

  defp format_error(job, worker, reason, stacktrace) when is_tuple(reason) do
    format_error(job, worker, PerformError.exception({worker, reason}), stacktrace)
  end

  defp format_error(job, _worker, error, stacktrace) do
    {blamed, stacktrace} = Exception.blame(:error, error, stacktrace)

    formatted = Exception.format(:error, blamed, stacktrace)

    %{attempt: job.attempt, at: utc_now(), error: formatted}
  end
end

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
  or `timeout` are omitted they fall back to their defaults: 100 for `size` and 1000ms for
  `timeout`. To process larger batches less frequently, we can increase both values:

      use Oban.Pro.Workers.Chunk, size: 500, timeout: :timer.seconds(5)

  Now chunks will run with up to 500 jobs or every 5 seconds, whichever comes first.

  Like other Pro workers, you define a `process/1` callback rather than `perform/1`. The `Chunk`
  worker's `process/1` is a little different, as it receives a list of jobs rather than a single
  struct. Fittingly, the [expected return values](#t:result/0) are different as well.

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

  In the event of an ephemeral crash, like a network issue, the entire batch may be retried if
  there are any remaining attempts.

  ## Chunk Organization

  Chunks are ran by a leader job (which has nothing to do with peer leadership). When the leader
  executes it determines whether a complete chunk is available or if enough time has elapsed to
  run anyhow. If neither case applies then the leader will delay until the timeout elapsed and
  execute with as many jobs as it can find.

  Only the leader job may be cancelled as it is the only one tracked by a producer. Cancelling any
  other jobs in the chunk won't stop the chunk from running.
  """

  import Ecto.Query, only: [limit: 2, order_by: 2, select: 3, where: 3]
  import DateTime, only: [utc_now: 0]

  alias Ecto.Multi
  alias Oban.{CrashError, Job, PerformError, Repo}
  alias Oban.Engines.Basic

  @type jobs :: [Job.t()]
  @type reason :: term()

  @type sub_result :: {reason(), jobs()}

  @type result ::
          :ok
          | {:ok, term()}
          | {:cancel | :discard | :error, reason(), jobs()}
          | [cancel: sub_result(), discard: sub_result(), error: sub_result()]

  defmacro __using__(opts) do
    {chunk_opts, stand_opts} = Keyword.split(opts, [:size, :sleep, :timeout])

    quote location: :keep do
      use Oban.Pro.Worker, unquote(stand_opts)

      alias Oban.Pro.Workers.Chunk

      @default_meta %{
        chunk_size: Keyword.get(unquote(chunk_opts), :size, 100),
        chunk_sleep: Keyword.get(unquote(chunk_opts), :sleep, 1000),
        chunk_timeout: Keyword.get(unquote(chunk_opts), :timeout, 1000)
      }

      @impl Oban.Worker
      def new(args, opts) when is_map(args) and is_list(opts) do
        opts = Keyword.update(opts, :meta, @default_meta, &Map.merge(@default_meta, &1))

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

  # Processing Helpers

  defp fetch_and_process(conf, worker, %{meta: %{"chunk_size" => size}} = job) do
    {:ok, meta} = Basic.init(conf, queue: job.queue, limit: size - 1)
    {:ok, {_meta, chunk}} = Basic.fetch_jobs(conf, meta, %{})

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

  # Check Helpers

  defp fetch_last_ts(conf, %{id: id, worker: worker}) do
    query =
      Job
      |> where([j], j.worker == ^worker)
      |> where([j], j.id != ^id)
      |> where([j], j.state != "scheduled")
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

  defp full_chunk?(conf, %{meta: %{"chunk_size" => size}, worker: worker}) do
    query =
      Job
      |> where([j], j.worker == ^worker)
      |> where([j], j.state == "available")
      |> select([j], count(j.id) >= ^(size - 1))

    Repo.one(conf, query)
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

  # Ack Helpers

  defp ack_completed(conf, jobs, result) do
    Repo.update_all(conf, ids_query(jobs), com_ups())

    result
  end

  defp ack_raised(conf, worker, chunk, job, error, stacktrace) do
    {dis, ers} = Enum.split_with(chunk, &exhausted?/1)

    multi =
      Multi.new()
      |> Multi.update_all(:err, ids_query(ers), err_ups(worker, job, error, stacktrace))
      |> Multi.update_all(:dis, ids_query(dis), dis_ups(worker, job, error, stacktrace))

    Repo.transaction(conf, multi)

    :ok
  end

  defp ack_errored(conf, worker, job, chunk, errored, reason) do
    {ers, oks, set} = split_with_set(errored, chunk)
    {dis, ers} = Enum.split_with(ers, &exhausted?/1)

    multi =
      Multi.new()
      |> Multi.update_all(:com, ids_query(oks), com_ups())
      |> Multi.update_all(:err, ids_query(ers), err_ups(worker, job, {:error, reason}))
      |> Multi.update_all(:dis, ids_query(dis), dis_ups(worker, job, {:error, reason}))

    with {:ok, _result} <- Repo.transaction(conf, multi) do
      if MapSet.member?(set, job.id), do: {:error, reason}, else: :ok
    end
  end

  defp ack_cancelled(conf, worker, job, chunk, cancelled, reason) do
    {can, oks, set} = split_with_set(cancelled, chunk)

    multi =
      Multi.new()
      |> Multi.update_all(:com, ids_query(oks), com_ups())
      |> Multi.update_all(:can, ids_query(can), can_ups(worker, job, {:cancel, reason}))

    with {:ok, _result} <- Repo.transaction(conf, multi) do
      if MapSet.member?(set, job.id), do: {:cancel, reason}, else: :ok
    end
  end

  defp ack_discarded(conf, worker, job, chunk, discarded, reason) do
    {dis, oks, set} = split_with_set(discarded, chunk)

    multi =
      Multi.new()
      |> Multi.update_all(:com, ids_query(oks), com_ups())
      |> Multi.update_all(:dis, ids_query(dis), dis_ups(worker, job, {:discard, reason}))

    with {:ok, _result} <- Repo.transaction(conf, multi) do
      if MapSet.member?(set, job.id), do: {:discard, reason}, else: :ok
    end
  end

  defp ack_multiple(conf, worker, host, chunk, multiple) do
    {oks, result} = split_multiple_result(host, chunk, multiple)

    group = fn {oper, reason, %{attempt: attempt}} -> {oper, reason, attempt} end
    multi = Multi.update_all(Multi.new(), :com, ids_query(oks), com_ups())

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

        Multi.update_all(acc, {oper, index}, ids_query(jobs), update)
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

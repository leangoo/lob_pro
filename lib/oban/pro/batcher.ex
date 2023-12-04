defmodule Oban.Pro.Batcher do
  @moduledoc false

  import Ecto.Query, only: [distinct: 2, select: 3, union: 2, where: 3]

  alias Oban.Pro.Engines.Smart
  alias Oban.{Job, Repo, Worker}

  require Logger

  @default_debounce 100

  @callbacks_to_functions %{
    "attempted" => :handle_attempted,
    "cancelled" => :handle_cancelled,
    "completed" => :handle_completed,
    "discarded" => :handle_discarded,
    "exhausted" => :handle_exhausted,
    "retryable" => :handle_retryable
  }

  @callbacks_to_states %{
    "attempted" => ~w(scheduled available executing),
    "completed" => ~w(scheduled available executing retryable cancelled discarded),
    "cancelled" => ~w(cancelled),
    "discarded" => ~w(discarded),
    "exhausted" => ~w(scheduled retryable available executing),
    "retryable" => ~w(retryable)
  }

  def on_start do
    tab = :ets.new(:batcher, [:public, write_concurrency: true])

    events = [
      [:oban, :job, :stop],
      [:oban, :job, :exception],
      [:oban, :engine, :cancel_job, :stop],
      [:oban, :engine, :cancel_all_jobs, :stop]
    ]

    :telemetry.attach_many(
      "oban.batch",
      events,
      &__MODULE__.handle_event/4,
      tab
    )
  end

  def on_stop do
    :telemetry.detach("oban.batch")
  end

  def handle_event(_event, _timing, %{conf: conf, job: job}, tab) do
    maybe_debounce(job, conf, tab)
  end

  def handle_event(_event, _timing, %{conf: conf, jobs: jobs}, tab) do
    Enum.each(jobs, &maybe_debounce(&1, conf, tab))
  end

  defp maybe_debounce(job, conf, tab) do
    case job.meta do
      %{"batch_id" => _, "callback" => _} ->
        :ok

      %{"batch_id" => batch_id} ->
        delay = Map.get(job.meta, "batch_debounce", @default_debounce)

        debounce(tab, batch_id, delay, fn -> check_and_insert(job, batch_id, conf) end)

      _ ->
        :ok
    end
  catch
    kind, value ->
      Logger.error(fn ->
        "[Oban.Pro.Workers.Batch] handler error: " <>
          Exception.format(kind, value, __STACKTRACE__)
      end)

      :ok
  end

  defp debounce(_tab, _key, 0, fun), do: fun.()

  defp debounce(tab, key, delay, fun) do
    if :ets.insert_new(tab, {key}) do
      Process.sleep(delay)
      :ets.delete(tab, key)
      fun.()
    else
      :ok
    end
  end

  defp check_and_insert(job, batch_id, conf) do
    batch_worker = Map.get(job.meta, "batch_callback_worker", job.worker)

    with {:ok, worker} <- Worker.from_string(batch_worker),
         supported = supported_callbacks(worker),
         {:ok, {states, exists}} <- states_for_callbacks(supported, batch_id, conf) do
      for callback <- supported,
          callback not in exists,
          callback_ready?(callback, states) do
        insert_callback(callback, batch_id, worker, job, conf)
      end
    end
  end

  defp supported_callbacks(worker) do
    for {name, func} <- @callbacks_to_functions,
        function_exported?(worker, func, 1),
        do: name
  end

  defp states_for_callbacks([], _batch_id, _conf), do: :ok

  defp states_for_callbacks(callbacks, batch_id, conf) do
    states =
      callbacks
      |> Enum.flat_map(&Map.fetch!(@callbacks_to_states, &1))
      |> Enum.uniq()

    match = %{batch_id: batch_id}

    state_query =
      Job
      |> select([j], j.state)
      |> distinct(true)
      |> where([j], fragment("? @> ?", j.meta, ^match))
      |> where([j], not fragment("? \\? ?", j.meta, "callback"))
      |> where([j], j.state in ^states)

    exist_query =
      Enum.reduce(callbacks, :none, fn callback, acc ->
        query =
          Job
          |> select([_j], [^callback])
          |> where([j], fragment("? @> ?", j.meta, ^Map.put(match, :callback, callback)))

        if acc == :none, do: query, else: union(acc, ^query)
      end)

    Repo.transaction(conf, fn ->
      states = Repo.all(conf, state_query)
      exists = Repo.all(conf, exist_query)

      {states, exists}
    end)
  end

  defp callback_ready?(callback, batch_states) do
    ready? =
      @callbacks_to_states
      |> Map.fetch!(callback)
      |> Enum.any?(&(&1 in batch_states))

    # Other callbacks use a negated query to avoid counting `completed` jobs.
    if callback in ~w(cancelled discarded retryable) do
      ready?
    else
      Kernel.not(ready?)
    end
  end

  defp insert_callback(callback, batch_id, worker, job, conf) do
    batch_args = Map.get(job.meta, "batch_callback_args", %{})
    batch_meta = Map.get(job.meta, "batch_callback_meta", %{})
    batch_queue = Map.get(job.meta, "batch_callback_queue", job.queue)

    meta = Map.merge(batch_meta, %{callback: callback, batch_id: batch_id})

    unique = [
      period: :infinity,
      fields: [:worker, :queue, :meta],
      keys: [:batch_id, :callback],
      states: Job.states()
    ]

    changeset = worker.new(batch_args, meta: meta, queue: batch_queue, unique: unique)

    if not changeset.valid? and Keyword.has_key?(changeset.errors, :args) do
      changeset
      |> structured_error_message()
      |> Logger.error()
    else
      {:ok, Smart.insert_job(conf, changeset, [])}
    end
  end

  defp structured_error_message(changeset) do
    """
    [Oban.Pro.Workers.Batch] can't insert batch callback because it has invalid keys:

      #{get_in(changeset.errors, [:args, Access.elem(0)])}

    Use one of the following options to restore batch callbacks:

    * Modify structured `keys` or `required` to allow the missing keys
    * Include the required arguments with the `batch_callback_args` option
    * Specify a different callback worker with the `batch_callback_worker` option
    """
  end
end

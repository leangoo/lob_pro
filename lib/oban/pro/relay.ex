defmodule Oban.Pro.Relay do
  @moduledoc """
  The `Relay` extension lets you insert and await the results of jobs locally or
  remotely, across any number of nodes, so you can seamlessly distribute jobs and
  await the results synchronously.

  Think of `Relay` as persistent, distributed tasks.

  `Relay` uses PubSub for to transmit results. That means it will work without Erlang distribution
  or clustering, but it does require Oban to have functional PubSub. It doesn't matter which node
  executes a job, the result will still be broadcast back.

  Results are encoded using `term_to_binary/2` and decoded using `binary_to_term/2` using the
  `:safe` option to prevent the creation of new atoms or function references. If you're returning
  results with atoms you _must be sure_ that those atoms are defined locally, where the `await/2`
  or `await_many/2` function is called.

  ## Usage

  Use `async/1` to insert a job for asynchronous execution:

  ```elixir
  relay =
    %{id: 123}
    |> MyApp.Worker.new()
    |> Oban.Pro.Relay.async()
  ```

  After inserting a job and constructing a relay, use `await/1` to await the job's execution and
  return the result:

  ```elixir
  {:ok, result} =
    %{:ok, 4}
    |> MyApp.Worker.new()
    |> Oban.Pro.Relay.async()
    |> Oban.Pro.Relay.await()
  ```

  By default, `await/1` will timeout after 5 seconds and return an `{:error, :timeout}` tuple. The job
  itself may continue to run, only the local process stops waiting on it. Pass an explicit timeout
  to wait longer:

  ```elixir
  {:ok, result} = Oban.Pro.Relay.await(relay, :timer.seconds(30))
  ```

  Any successful result such as `:ok`, `{:ok, value}`, or `{:cancel, reason}` is passed back as
  the await result. When the executed job returns an `{:error, reason}` tuple, raises an
  exception, or crashes, the result comes through as an error tuple.

  Use `await_many/1` when you need the results of multiple async relays:

  ```elixir
  relayed =
    1..3
    |> Enum.map(&DoubleWorker.new(%{int: &1}))
    |> Enum.map(&Oban.Pro.Relay.async(&1))
    |> Oban.Pro.Relay.await_many()

  #=> [{:ok, 2}, {:ok, 4}, {:ok, 6}]
  ```
  """

  alias Ecto.{Changeset, UUID}
  alias Oban.Pro.Utils
  alias Oban.{Job, Notifier}

  require Logger

  @type t :: %__MODULE__{job: Job.t(), pid: pid(), ref: UUID.t()}

  @type await_result ::
          :ok
          | {:ok, term()}
          | {:cancel, term()}
          | {:discard, term()}
          | {:error, :result_too_large | :timeout | Exception.t()}
          | {:snooze, integer()}

  defstruct [:job, :pid, :ref]

  @postgres_max_bytes 8000

  @doc """
  Insert a job for asynchronous execution.

  The returned map contains the caller's pid and a unique ref that is used to await the results.

  ## Examples

  The single arity version takes a job changeset and inserts it:

      relay =
        %{id: 123}
        |> MyApp.Worker.new()
        |> Oban.Pro.Relay.async()

  When the Oban instance has a custom name, or an app has multiple Oban
  instances, you can use the two arity version to select an instance:

      changeset = MyApp.Worker.new(%{id: 123})
      Oban.Pro.Relay.async(MyOban, changeset)
  """
  @spec async(Oban.name(), Job.changeset()) :: t() | {:error, Job.changeset()}
  def async(name \\ Oban, %Changeset{} = changeset) do
    pid = self()
    ref = Utils.to_uniq_key(changeset) || UUID.generate()

    meta =
      changeset
      |> Changeset.get_change(:meta, %{})
      |> Map.put_new(:await_ref, ref)

    changeset = Changeset.put_change(changeset, :meta, meta)

    :ok = Notifier.listen(name, [:gossip])

    with {:ok, job} <- Oban.insert(name, changeset) do
      %__MODULE__{job: job, pid: pid, ref: ref}
    end
  end

  @doc """
  Await a relay's execution and return the result.

  Any successful result such as `:ok`, `{:ok, value}`, or `{:cancel, reason}` is passed back as
  the await result. When the executed job returns an `{:error, reason}` tuple, raises an
  exception, or crashes, the result comes back as an error tuple with the exception.

  By default, `await/1` will timeout after 5 seconds and return `{:error, :timeout}`. The job
  itself may continue to run, only the local process stops waiting on it.

  > #### Result Size Limits {: .warning}
  >
  > When using the default `Oban.Notifiers.Postgres` notifier for PubSub, any value larger than 8kb
  > (compressed) can't be broadcast due to a Postgres `NOTIFY` limitation. Instead, awaiting will
  > return an `{:error, :result_too_large}` tuple. The `Oban.Notifiers.PG` notifier doesn't have any
  > such size limitation, but it requires Distributed Erlang.

  ## Examples

  Await a job:

      {:ok, result} =
        %{id: 123}
        |> MyApp.Worker.new()
        |> Oban.Pro.Relay.async()
        |> Oban.Pro.Relay.await()

  Increase the wait time with a timeout value:

      %{id: 456}
      |> MyApp.Worker.new()
      |> Oban.Pro.Relay.async()
      |> Oban.Pro.Relay.await(:timer.seconds(30))
  """
  @spec await(t(), timeout()) :: await_result()
  def await(%{pid: pid, ref: ref}, timeout \\ 5_000) do
    check_ownership!(pid)

    receive do
      {:notification, :gossip, %{"ref" => ^ref, "result" => result}} ->
        Utils.decode64(result)
    after
      timeout -> {:error, :timeout}
    end
  end

  @doc """
  Await replies from multiple relays and return the results.

  It returns a list of the results in the same order as the relays supplied as the first argument.

  Unlike `Task.await_many` or `Task.yield_many`, `await_many/2` may return partial results when
  the timeout is reached. When a job hasn't finished executing the value will be `{:error,
  :timeout}`

  ## Examples

  Await multiple jobs without any errors or timeouts:

      relayed =
        1..3
        |> Enum.map(&DoubleWorker.new(%{int: &1}))
        |> Enum.map(&Oban.Pro.Relay.async(&1))
        |> Oban.Pro.Relay.await_many()

      #=> [{:ok, 2}, {:ok, 4}, {:ok, 6}]

  Await multiple jobs with an error timeout:

      relayed =
        [1, 2, 300_000_000]
        |> Enum.map(&SlowWorker.new(%{int: &1}))
        |> Enum.map(&Oban.Pro.Relay.async(&1))
        |> Oban.Pro.Relay.await_many(100)

      #=> [{:ok, 2}, {:ok, 4}, {:error, :timeout}]
  """
  @spec await_many([t()], timeout()) :: [await_result()]
  def await_many([%__MODULE__{} | _] = relays, timeout \\ 5_000) do
    awaited =
      for %{pid: pid, ref: ref} <- relays, into: %{} do
        check_ownership!(pid)

        {ref, {:error, :timeout}}
      end

    error_ref = make_ref()
    timer_ref = maybe_send_after(error_ref, timeout)

    try do
      await_many(relays, awaited, %{}, error_ref)
    after
      if is_reference(timer_ref), do: Process.cancel_timer(timer_ref)

      receive do: (^error_ref -> :ok), after: (0 -> :ok)
    end
  end

  defp await_many(relays, awaited, replied, _error_ref) when map_size(awaited) == 0 do
    for %{ref: ref} <- relays, do: Map.fetch!(replied, ref)
  end

  defp await_many(relays, awaited, replied, error_ref) do
    receive do
      ^error_ref ->
        for %{ref: ref} <- relays, do: replied[ref] || awaited[ref]

      {:notification, :gossip, %{"ref" => ref, "result" => res}} ->
        if Map.has_key?(awaited, ref) do
          await_many(
            relays,
            Map.delete(awaited, ref),
            Map.put(replied, ref, Utils.decode64(res)),
            error_ref
          )
        else
          await_many(relays, awaited, replied, error_ref)
        end
    end
  end

  @doc false
  def handle_event([:oban, :job, _], _, %{conf: conf, job: job} = meta, _) do
    with %{"await_ref" => aref} <- job.meta do
      payload = %{"ref" => aref, "result" => extract_result(conf, meta)}

      Notifier.notify(conf, :gossip, payload)
    end
  catch
    kind, value ->
      Logger.error(fn ->
        "[Oban.Pro.Relay] handler error: " <> Exception.format(kind, value)
      end)

      :ok
  end

  @doc false
  def on_start do
    events = [
      [:oban, :job, :stop],
      [:oban, :job, :exception]
    ]

    :telemetry.attach_many("oban.pro.relay", events, &__MODULE__.handle_event/4, nil)
  end

  @doc false
  def on_stop do
    :telemetry.detach("oban.pro.relay")
  end

  # Messaging

  defp check_ownership!(pid) do
    unless pid == self() do
      raise ArgumentError,
            "relay must be awaited by #{inspect(pid)}, was awaited by #{inspect(self())}"
    end
  end

  defp maybe_send_after(_error_ref, :infinity), do: nil

  defp maybe_send_after(error_ref, timeout) do
    Process.send_after(self(), error_ref, timeout)
  end

  # Result Handling

  defp extract_result(conf, %{state: state, result: result})
       when state in [:cancelled, :snoozed, :success] do
    encoded = Utils.encode64(result)

    if conf.notifier == Oban.Notifiers.Postgres and byte_size(encoded) > @postgres_max_bytes do
      Utils.encode64({:error, :result_too_large})
    else
      encoded
    end
  end

  defp extract_result(_conf, %{state: :discard, job: job}) do
    Utils.encode64({:discard, job.unsaved_error.reason})
  end

  defp extract_result(_conf, %{state: :failure, error: error}) do
    Utils.encode64({:error, error})
  end
end

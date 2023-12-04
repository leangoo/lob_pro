defmodule Oban.Pro.Workers.Chain do
  @moduledoc """
  Chain workers link jobs together to ensure they run in a strict sequential order. Downstream
  jobs automatically suspending execution until the upstream job is `completed`. Behaviour in the
  event of cancellation or discards is customizable to allow for uninterrupted processing or
  holding to wait for outside intervention.

  Jobs in a chain only run after the previous job completes successfully, regardless of snoozing
  or retries.

  ## Usage

  Chains are appropriate in situations where jobs are used to synchronize internal state with
  outside state via events. For example, imagine a system that relies on webhooks from a payment
  processor to track account balance.

  ```elixir
  defmodule MyApp.WebhookWorker do
    use Oban.Pro.Workers.Chain, queue: :webhooks, by: :worker

    @impl true
    def process(%Job{args: %{"account_id" => account_id, "event" => event}}) do
      account_id
      |> MyApp.Account.find()
      |> MyApp.Account.handle_webhook_event(event)
    end
  end
  ```

  It's essential that jobs for an account are processed in order, while jobs from separate
  accounts can run concurrently. Modify the `:by` option to partition by worker and `:account_id`:

  ```elixir
  defmodule MyApp.WebhookWorker do
    use Oban.Pro.Workers.Chain, queue: :webhooks, by: [:worker, args: :account_id]

    ...
  ```

  Now webhooks for each account are guaranteed to run in order regardless of queue concurrency or
  errors. See [chain partitioning](#chain-partitioning) for more partitioning examples.

  ## Chain Partitioning

  By default, chains are sequenced by `worker`, which means any job with the same worker forms a
  chain. This approach may not always be suitable. For instance, you may want to link workers
  based on a field like `:account_id` instead of just the worker. In such cases, you can use the
  [`:by`](#t:chain_by/0) option to customize how chains are partitioned.

  Here are a few examples of using `:by` to achieve fine-grained control over chain partitioning:

  ```elixir
  # Explicitly chain by :worker
  use Oban.Pro.Workers.Chain, by: :worker

  # Chain by a single args key without considering the worker
  use Oban.Pro.Workers.Chain, by: [args: :account_id]

  # Chain by multiple args keys without considering the worker
  use Oban.Pro.Workers.Chain, by: [args: [:account_id, :cohort]]

  # Chain by worker and a single args key
  use Oban.Pro.Workers.Chain, by: [:worker, args: :account_id]

  # Chain by worker and multiple args key
  use Oban.Pro.Workers.Chain, by: [:worker, args: [:account_id, :cohort]]

  # Chain by a single meta key
  use Oban.Pro.Workers.Chain, by: [meta: :source_id]
  ```

  ## Handling Cancelled/Discarded

  The way a chain behaves when jobs are cancelled or discarded is customizable with the
  `:on_discarded` and `:on_cancelled` options.

  There are three strategies for handling upstream discards and cancellations:

  * `:ignore` — keep processing jobs in the chain as if upstream `cancelled` or `discarded` jobs
    completed successfully. This is the default behaviour.

  * `:hold` — stop processing any jobs in the chain until the `cancelled` or `discarded` job is
    `completed` or eventually deleted.

  * `:halt` — cancel jobs in the chain when an upstream job is `cancelled` or `discarded`.
    Cancelling cascades until jobs are retried via manual intervention, e.g. with the Web
    dashboard or `Oban.retry_all_jobs/1`.

    Because halting cancels a job, you must use `on_discarded: :halt` along with `on_cancelled:
    :halt` to fully stop a chain.

  Here's an example of a chain that halts on `discarded` and holds on `cancelled`:

  ```elixir
  use Oban.Pro.Workers.Chain, on_discarded: :halt, on_cancelled: :hold
  ```

  Holding a job snoozes it briefly while it awaits manual intervention. The default snooze period
  is 60s, and you can customize it through the `:hold_snooze` option. Here we're bumping snooze up
  to 600s (10 minutes):

  ```elixir
  use Oban.Pro.Workers.Chain, on_cancelled: :hold, hold_snooze: 600
  ```

  ## Customizing Chains

  Chains use conservative defaults for safe, and relatively quick, dependency resolution. You can
  customize waiting times and retry intensity by providing a few top-level options:

  * `:wait_sleep` — the number of milliseconds to wait between checks. This value, combined with
    `:wait_retry`, dictates how long a job waits before snoozing. Defaults to `1000ms`.

  * `:wait_retry` — the number of times to retry a check between sleeping. This value, combined
    with `:wait_sleep`, dictates how long a job waits before snoozing. Defaults to `10`.

  * `:wait_snooze` — the number of seconds a job will snooze after awaiting `executing` upstream
    dependencies. If upstream dependencies are `scheduled` or `retryable` then the job snoozes until
    the latest dependency's `scheduled_at` timestamp. Defaults to `5`.
  """

  import Ecto.Query

  alias Oban.{Job, Repo, Validation}
  alias Oban.Pro.Utils

  @type key_or_keys :: atom() | [atom()]

  @type chain_by ::
          :worker
          | {:args, key_or_keys()}
          | {:meta, key_or_keys()}
          | [:worker | {:args, key_or_keys()} | {:meta, key_or_keys()}]

  @type on_action :: :halt | :hold | :ignore

  @type options :: [
          by: chain_by(),
          hold_snooze: pos_integer(),
          on_cancelled: on_action(),
          on_discarded: on_action(),
          wait_retry: pos_integer(),
          wait_sleep: timeout(),
          wait_snooze: pos_integer()
        ]

  @default_meta %{
    on_discarded: "ignore",
    on_cancelled: "ignore",
    chain_key: nil,
    hold_snooze: 60,
    wait_retry: 10,
    wait_sleep: 1000,
    wait_snooze: 5
  }

  @opts_keys ~w(by hold_snooze on_cancelled on_discarded wait_retry wait_sleep wait_snooze)a

  # Purely used for validation
  defstruct @opts_keys

  @doc false
  defmacro __using__(opts) do
    {chain_opts, other_opts} = Keyword.split(opts, @opts_keys)

    quote do
      Validation.validate!(unquote(chain_opts), &Oban.Pro.Workers.Chain.validate/1)

      use Oban.Pro.Worker, unquote(other_opts)

      alias Oban.Pro.Workers.Chain

      @base_meta unquote(chain_opts)
                 |> Keyword.delete(:by)
                 |> Map.new()

      @impl Oban.Worker
      def new(args, opts) when is_map(args) and is_list(opts) do
        meta =
          opts
          |> Keyword.get(:meta, %{})
          |> Map.merge(@base_meta)
          |> Map.put_new(:chain_by, Keyword.fetch!(unquote(chain_opts), :by))
          |> Map.update!(:chain_by, &Utils.normalize_by/1)

        meta = Map.put(meta, :chain_key, Chain.to_chain_key(__MODULE__, args, meta))

        super(args, Keyword.put(opts, :meta, meta))
      end

      @impl Oban.Worker
      def perform(%Job{} = job) do
        opts = __opts__()

        with {:ok, job} <- Oban.Pro.Worker.before_process(job, opts) do
          job
          |> Chain.maybe_process(__MODULE__)
          |> Oban.Pro.Worker.after_process(job, opts)
        end
      end
    end
  end

  @doc false
  def validate(opts) do
    Validation.validate(opts, fn
      {:by, by} -> Oban.Pro.Validation.validate_by(by)
      {:hold_snooze, snooze} -> Validation.validate_integer(:snooze, snooze)
      {:on_cancelled, action} -> validate_on_action(:on_cancelled, action)
      {:on_discarded, action} -> validate_on_action(:on_discarded, action)
      {:wait_retry, retry} -> Validation.validate_integer(:wait_retry, retry)
      {:wait_sleep, sleep} -> Validation.validate_timeout(:wait_sleep, sleep)
      {:wait_snooze, snooze} -> Validation.validate_integer(:wait_snooze, snooze)
      option -> {:unknown, option, __MODULE__}
    end)
  end

  defp validate_on_action(_key, :halt), do: :ok
  defp validate_on_action(_key, :hold), do: :ok
  defp validate_on_action(_key, :ignore), do: :ok

  defp validate_on_action(key, action) do
    {:error, "expected #{key} to be :halt, :hold, or :ignore, got: #{inspect(action)}"}
  end

  @doc false
  def to_chain_key(worker, args, meta) do
    meta
    |> Map.fetch!(:chain_by)
    |> List.wrap()
    |> Enum.reduce(%{}, fn
      :worker, acc -> Map.put(acc, :worker, worker)
      [:args, keys], acc -> Map.put(acc, :args, take_keys(args, keys))
      [:meta, keys], acc -> Map.put(acc, :meta, take_keys(meta, keys))
    end)
    |> :erlang.phash2()
  end

  defp take_keys(map, keys) do
    keys
    |> Enum.sort()
    |> Enum.map(fn key -> map[key] || map[to_string(key)] end)
  end

  @doc false
  def maybe_process(%Job{conf: conf, id: id, meta: meta} = job, worker, wait_count \\ 0) do
    meta = meta_with_defaults(meta)

    query =
      Job
      |> select([j], {j.state, j.scheduled_at})
      |> where([j], fragment("? @> ?", j.meta, ^%{chain_key: meta.chain_key}))
      |> where([j], j.state != "completed")
      |> where([j], j.id < ^id)
      |> order_by(desc: :id)
      |> limit(1)

    case Repo.one(conf, query) do
      {"executing", _at} ->
        if wait_count >= meta.wait_retry do
          {:snooze, meta.wait_snooze}
        else
          Process.sleep(meta.wait_sleep)

          maybe_process(job, worker, wait_count + 1)
        end

      {state, _at} when state in ~w(cancelled discarded) ->
        key = if state == "cancelled", do: :on_cancelled, else: :on_discarded

        case Map.fetch!(meta, key) do
          "halt" -> {:cancel, "chain halted by upstream job"}
          "hold" -> {:snooze, meta.hold_snooze}
          "ignore" -> worker.process(job)
        end

      {_state, scheduled_at} ->
        seconds =
          scheduled_at
          |> DateTime.diff(DateTime.utc_now())
          |> max(meta.wait_snooze)

        {:snooze, seconds}

      nil ->
        worker.process(job)
    end
  end

  defp meta_with_defaults(meta) do
    Map.new(@default_meta, fn {key, val} -> {key, meta[to_string(key)] || val} end)
  end
end

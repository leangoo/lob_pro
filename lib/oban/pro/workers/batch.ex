defmodule Oban.Pro.Workers.Batch do
  @moduledoc ~S"""
  A `Batch` worker links the execution of many jobs as a group and runs optional
  callbacks after jobs are processed. This allows your application to coordinate
  the execution of tens, hundreds or thousands of jobs in parallel.

  ## Usage

  Let's define a worker that delivers daily emails in a large batch:

      defmodule MyApp.EmailBatch do
        use Oban.Pro.Workers.Batch, queue: :mailers

        @impl true
        def process(%Job{args: %{"email" => email}}) do
          MyApp.Mailer.daily_update(email)
        end
      end

  Note that we **define a `process/1` callback instead of `perform/1`** because
  `perform/1` is used behind the scenes to coordinate regular execution and
  callbacks within the same worker. The `process/1` function receives an
  `Oban.Job` struct, just like `perform/1` would and it should return the same
  accepted values, i.e. `:ok`, `{:ok, value}`, `{:error, error}`.

  The `process/1` function above only looks for an `"email"` key in the job args,
  but a `"batch_id"` is also available in `meta`. We'll modify the function to
  extract the `batch_id` as well:

      def process(%Job{args: %{"email" => email}, meta: %{"batch_id" => batch_id}}) do
        {:ok, reply} = MyApp.Mailer.daily_update(email)

        track_delivery(batch_id, reply)

        :ok
      end

  Now the hypothetical `track_delivery/2` function will store the delivery details
  for retrieval later, possibly by one of our [handler callbacks](#handler-callbacks).

  ## Inserting Batches

  Create batches with `c:new_batch/1,2` by passing a list of args and options, or a
  list of heterogeneous jobs. A list of args and options is transformed into a list
  of jobs for that batch module. For example, this will build and insert two
  `EmailBatch` jobs:

      [%{email: "foo@example.com"}, %{email: "bar@example.com"}]
      |> MyApp.EmailBatch.new_batch()
      |> Oban.insert_all()

  To schedule a batch in the future, or override default options, you pass a list
  of options:

      [%{email: "foo@example.com"}, %{email: "bar@example.com"}]
      |> MyApp.EmailBatch.new_batch(schedule_in: 60, priority: 1, max_attempts: 3)
      |> Oban.insert_all()

  The `c:new_batch/1,2` callback automatically injects a unique `batch_id` into each
  job's meta. A `Batch` worker is a regular `Oban.Worker` under the hood, which
  means you can use `new/2` to insert jobs as well, provided you use a
  [deterministic batch id](#generating-batch-ids).

  Creating a heterogeneous batch is similar, though you must provide an explicit worker for
  callbacks:

      mail_jobs = Enum.map(mail_args, &MyApp.MailWorker.new/1)
      push_jobs = Enum.map(push_args, &MyApp.PushWorker.new/1)

      MyApp.BatchWorker.new_batch(
        mail_jobs ++ push_jobs,
        batch_callback_worker: MyApp.CallbackWorker
      )

  ## Generating Batch IDs

  By default a `batch_id` is generated as a version 4 random [UUID][uuid]. UUIDs
  are more than sufficient to ensure that batches are unique between workers and
  nodes for any period. However, if you require control, you can override
  `batch_id` generation at the worker level or pass a value directly to the
  `new_batch/2` function.

  To override the `batch_id` for a particular worker you override the `c:gen_id/0`
  callback:

      defmodule MyApp.BatchWorker do
        use Oban.Pro.Workers.Batch

        # Generate a 24 character long random string instead
        @impl Batch
        def gen_id do
          24
          |> :crypto.strong_rand_bytes()
          |> Base.encode64()
        end
      end

  The `c:gen_id/0` callback is suited for random/non-deterministic id generation. If
  you'd prefer to use a deterministic id instead, you can pass the `batch_id` in as
  an option to `new_batch/2`:

      MyApp.BatchWorker.new_batch(list_of_args, batch_id: "custom-batch-id")

  Using this technique you can verify the `batch_id` in tests or append to the
  batch manually after it was originally created. For example, you can add to a
  batch that is scheduled for the future:

      batch_id = "daily-batch-#{Date.utc_today()}"
      midnight =
        Date.utc_today()
        |> NaiveDateTime.new(~T[11:59:59])
        |> elem(1)
        |> DateTime.from_naive!("Etc/UTC")

      # Create the initial batch
      initial_args
      |> MyApp.BatchWorker.new_batch(batch_id: batch_id, schedule_at: midnight)
      |> Oban.insert_all()

      # Add items to the batch later in the day
      %{batch_id: batch_id, other_arg: "other"}
      |> MyApp.BatchWorker.new(schedule_at: midnight)
      |> Oban.insert()

  When batch jobs execute at midnight they'll all be tracked together.

  ## Handler Callbacks

  After jobs in the batch are processed, a callback job may be inserted There are four optional
  batch handler callbacks that a worker may define:

  * `c:handle_attempted/1` — called after **all jobs** in the batch were attempted at least once,
    regardless of whether they succeeded or not.

  * `c:handle_cancelled/1` — called after **the first job** in a batch has `cancelled` state.

  * `c:handle_completed/1` — called after **all jobs** in the batch have a `completed` state. This
    handler may never be called if one or more jobs keep failing or any are discarded.

  * `c:handle_discarded/1` — called after **the first job** in a batch has a `discarded` state.

  * `c:handle_exhausted/1` — called after **all jobs** in the batch have a `cancelled`,
    `completed` or `discarded` state.

  * `c:handle_retryable/1` — called after **the first job** in a batch has a `retryable` state.

  Each handler callback receives an `Oban.Job` struct with the `batch_id` in `meta` and should
  return `:ok`. The callbacks are executed as separate isolated jobs, so they may be retried or
  discarded like any other job.

  Here we'll implement each of the optional handler callbacks and have them print out the batch
  status along with the `batch_id`:

      defmodule MyApp.BatchWorker do
        use Oban.Pro.Workers.Batch

        @impl Oban.Pro.Worker
        def process(_job), do: :ok

        @impl Batch
        def handle_attempted(%Job{meta: %{"batch_id" => batch_id}}) do
          IO.inspect({:attempted,  batch_id})
        end

        @impl Batch
        def handle_cancelled(%Job{meta: %{"batch_id" => batch_id}}) do
          IO.inspect({:cancelled,  batch_id})
        end

        @impl Batch
        def handle_completed(%Job{meta: %{"batch_id" => batch_id}}) do
          IO.inspect({:completed,  batch_id})
        end

        @impl Batch
        def handle_discarded(%Job{meta: %{"batch_id" => batch_id}}) do
          IO.inspect({:discarded,  batch_id})
        end

        @impl Batch
        def handle_exhausted(%Job{meta: %{"batch_id" => batch_id}}) do
          IO.inspect({:exhausted,  batch_id})
        end

        @impl Batch
        def handle_retryable(%Job{meta: %{"batch_id" => batch_id}}) do
          IO.inspect({:retryable,  batch_id})
        end
      end

  ### Forwarding Callback Args or Meta

  By default, callback jobs have an empty `args` map. With the `:batch_callback_args` option to
  `new_batch/2`, you can pass custom args through to each callback. For example, here we're
  passing a webhook URLs in the args for use callbacks:

      MyBatch.new_batch(jobs, batch_callback_args: %{webhook: "https://web.hook"})

  Here, we're passing an open tracing span through `meta`:

      MyBatch.new_batch(jobs, batch_callback_meta: %{span: tracing_span})

  Any JSON encodable map may be passed to callbacks, but note that the complete map is stored in
  each batch job's meta.

  ### Alternate Callback Workers

  For some batches, notably those with heterogeneous jobs, it's handy to specify a different
  worker for callbacks. That is easily accomplished by passing the `:batch_callback_worker` option
  to `new_batch/2`:

      MyBatch.new(jobs, batch_callback_worker: MyCallbackWorker)

  The callback worker **must** be an `Oban.Worker` that defines one or more of the batch callback
  handlers.

  ### Alternate Callback Queues

  It's also possible to override the callback queue for a batch of jobs by passing the
  `:batch_callback_queue` option to `new_batch/2`:

      MyBatch.new(jobs, batch_callback_queue: :callbacks_only)

  ## Fetching Batch Jobs

  For map/reduce style workflows, or to pull more context from batch jobs, it's possible to load
  all jobs from the batch with `c:all_batch_jobs/1,2` and `c:stream_batch_jobs/1,2`. The functions
  take a single batch job and returns a list or stream of all non-callback jobs in the batch,
  which you can then operate on with `Enum` or `Stream` functions.

  As an example, imagine you have a batch that ran for a few thousand accounts and you'd like to
  notify admins that the batch is complete.

      defmodule MyApp.BatchWorker do
        use Oban.Pro.Workers.Batch

        @impl Batch
        def handle_completed(%Job{} = job) do
          {:ok, account_ids} =
            MyApp.Repo.transaction(fn ->
              job
              |> stream_batch_jobs()
              |> Enum.map(& &1.args["account_id"])
            end)

          account_ids
          |> MyApp.Accounts.all()
          |> MyApp.Mailer.notify_admins_about_batch()
        end

  Streaming is provided by Ecto's `Repo.stream`, and it must take place within a transaction.
  While it may be overkill for small batches, for batches with tens or hundreds of thousands of
  jobs, it will prevent massive memory spikes or the database grinding to a halt!

  [uuid]: https://en.wikipedia.org/wiki/Universally_unique_identifier
  """

  import Ecto.Query, only: [order_by: 2, where: 3]

  alias Ecto.Changeset
  alias Oban.{Job, Repo, Worker}

  require Logger

  @type args_or_jobs :: [Job.t() | Job.args()]

  @type batch_opts ::
          Job.option()
          | {:batch_id, String.t()}
          | {:batch_debounce, non_neg_integer()}
          | {:batch_callback_args, map()}
          | {:batch_callback_meta, map()}
          | {:batch_callback_queue, atom() | String.t()}
          | {:batch_callback_worker, module()}

  @type repo_opts :: {:timeout, timeout()}

  @doc """
  Build a collection of job changesets from the provided `args` and `opts`.

  ## Example

  Create a batch:

      [%{email: "foo@example.com"}, %{email: "bar@example.com"}]
      |> MyApp.EmailBatch.new_batch()
      |> Oban.insert_all()

  Schedule a batch in the future with some optional overrides:

      [%{email: "foo@example.com"}, %{email: "bar@example.com"}]
      |> MyApp.EmailBatch.new_batch(schedule_in: 60, priority: 1, max_attempts: 3)
      |> Oban.insert_all()

  Create a heterogeneous batch with an explicit callback worker:

      mail_jobs = Enum.map(mail_args, &MyApp.MailWorker.new/1)
      push_jobs = Enum.map(push_args, &MyApp.PushWorker.new/1)

      MyApp.BatchWorker.new_batch(
        mail_jobs ++ push_jobs,
        batch_callback_worker: MyApp.CallbackWorker
      )
  """
  @callback new_batch(args_or_jobs(), [batch_opts()]) :: [Changeset.t()]

  @doc """
  Generate a unique string to identify the batch.

  Defaults to a 128bit UUID.

  ## Example

  Generate a batch id using random bytes instead of a UUID:

      @impl Batch
      def gen_id do
        24
        |> :crypto.strong_rand_bytes()
        |> Base.encode64()
      end
  """
  @callback gen_id() :: String.t()

  @doc """
  Called after all jobs in the batch were attempted at least once.

  If a `handle_attempted/1` function is defined it is executed by an isolated callback job.

  ## Example

  Print when all jobs were attempted:

      @impl Batch
      def handle_attempted(%Job{meta: %{"batch_id" => batch_id}}) do
        IO.inspect(batch_id, label: "Attempted")
      end
  """
  @callback handle_attempted(job :: Job.t()) :: Worker.result()

  @doc """
  Called after any jobs in the batch have a `cancelled` state.

  If a `handle_cancelled/1` function is defined it is executed by an isolated callback job.

  ## Example

  Print when any jobs are discarded:

      @impl Batch
      def handle_cancelled(%Job{meta: %{"batch_id" => batch_id}}) do
        IO.inspect(batch_id, label: "Cancelled")
      end
  """
  @callback handle_cancelled(job :: Job.t()) :: Worker.result()

  @doc """
  Called after all jobs in the batch have a `completed` state.

  If a `handle_completed/1` function is defined it is executed by an isolated callback job.

  ## Example

  Print when all jobs are completed:

      @impl Batch
      def handle_completed(%Job{meta: %{"batch_id" => batch_id}}) do
        IO.inspect(batch_id, label: "Completed")
      end
  """
  @callback handle_completed(job :: Job.t()) :: Worker.result()

  @doc """
  Called after any jobs in the batch have a `discarded` state.

  If a `handle_discarded/1` function is defined it is executed by an isolated callback job.

  ## Example

  Print when any jobs are discarded:

      @impl Batch
      def handle_discarded(%Job{meta: %{"batch_id" => batch_id}}) do
        IO.inspect(batch_id, label: "Discarded")
      end
  """
  @callback handle_discarded(job :: Job.t()) :: Worker.result()

  @doc """
  Called after all jobs in the batch have either a `completed` or `discarded` state.

  If a `handle_exhausted/1` function is defined it is executed by an isolated callback job.

  ## Example

  Print when all jobs are completed or discarded:

      @impl Batch
      def handle_exhausted(%Job{meta: %{"batch_id" => batch_id}}) do
        IO.inspect(batch_id, label: "Exhausted")
      end
  """
  @callback handle_exhausted(job :: Job.t()) :: Worker.result()

  @doc """
  Called after any jobs in the batch have a `retryable` state.

  If a `handle_retryable/1` function is defined it is executed by an isolated callback job.

  ## Example

  Print when any jobs are retryable:

      @impl Batch
      def handle_retryable(%Job{meta: %{"batch_id" => batch_id}}) do
        IO.inspect(batch_id, label: "Retryable")
      end
  """
  @callback handle_retryable(job :: Job.t()) :: Worker.result()

  @doc """
  Get all non-callback jobs from a batch.

  ## Example

  Extract recorded results from all jobs in a batch:

      @impl Batch
      def handle_completed(%Job{} = job) do
        results =
          job
          |> all_batch_jobs()
          |> Enum.map(&fetch_recorded/1)

        MyApp.Mailer.notify_admins_about_results(results)
      end
  """
  @callback all_batch_jobs(Job.t(), [repo_opts()]) :: Enum.t()

  @doc """
  Stream non-callback jobs from a batch.

  ## Example

  Stream all batch jobs and extract an argument for reporting:

      @impl Batch
      def handle_completed(%Job{} = job) do
        {:ok, account_ids} =
          MyApp.Repo.transaction(fn ->
            job
            |> stream_batch_jobs()
            |> Enum.map(& &1.args["account_id"])
          end)

        account_ids
        |> MyApp.Accounts.all()
        |> MyApp.Mailer.notify_admins_about_batch()
      end
  """
  @callback stream_batch_jobs(Job.t(), [repo_opts()]) :: Enum.t()

  @optional_callbacks handle_attempted: 1,
                      handle_cancelled: 1,
                      handle_completed: 1,
                      handle_discarded: 1,
                      handle_exhausted: 1,
                      handle_retryable: 1

  @doc false
  defmacro __using__(opts) do
    quote location: :keep do
      use Oban.Pro.Worker, unquote(opts)

      alias Oban.Pro.Workers.Batch

      @behaviour Oban.Pro.Workers.Batch

      @batch_opts ~w(
        batch_id
        batch_debounce
        batch_callback_args
        batch_callback_meta
        batch_callback_queue
        batch_callback_worker
      )a

      @impl Batch
      def new_batch([_ | _] = args_or_jobs, opts \\ []) when is_list(opts) do
        {batch_opts, job_opts} = Keyword.split(opts, @batch_opts)

        batch_meta =
          batch_opts
          |> Map.new()
          |> Map.put_new(:batch_id, gen_id())

        Batch.new(__MODULE__, args_or_jobs, job_opts, batch_meta)
      end

      @impl Batch
      def gen_id do
        Ecto.UUID.generate()
      end

      @impl Batch
      def all_batch_jobs(job, opts \\ []) do
        Batch.all_jobs(job, opts)
      end

      @impl Batch
      def stream_batch_jobs(job, opts \\ []) do
        Batch.stream_jobs(job, opts)
      end

      @impl Oban.Worker
      def perform(job) do
        opts = __opts__()

        with {:ok, job} <- Oban.Pro.Worker.before_process(job, opts) do
          job
          |> Batch.maybe_process(__MODULE__)
          |> Oban.Pro.Worker.after_process(job, opts)
        end
      end

      defoverridable Batch
    end
  end

  # Batch Callbacks

  @doc false
  def new(worker, args_or_jobs, job_opts, batch_meta) do
    for args <- args_or_jobs do
      changeset = if match?(%_{}, args), do: args, else: worker.new(args, job_opts)

      meta =
        changeset
        |> Changeset.get_change(:meta, %{})
        |> Map.merge(batch_meta)

      Changeset.put_change(changeset, :meta, meta)
    end
  end

  @doc false
  def maybe_process(job, module) do
    case job.meta do
      %{"callback" => "attempted"} -> maybe_handle(module, :handle_attempted, job)
      %{"callback" => "cancelled"} -> maybe_handle(module, :handle_cancelled, job)
      %{"callback" => "completed"} -> maybe_handle(module, :handle_completed, job)
      %{"callback" => "discarded"} -> maybe_handle(module, :handle_discarded, job)
      %{"callback" => "exhausted"} -> maybe_handle(module, :handle_exhausted, job)
      %{"callback" => "retryable"} -> maybe_handle(module, :handle_retryable, job)
      _ -> module.process(job)
    end
  end

  defp maybe_handle(module, callback, job) do
    if function_exported?(module, callback, 1) do
      apply(module, callback, [job])
    else
      :ok
    end
  end

  @doc false
  def all_jobs(%Job{conf: conf, meta: %{"batch_id" => batch_id}}, opts) do
    Repo.all(conf, batch_query(batch_id), opts)
  end

  @doc false
  def stream_jobs(%Job{conf: conf, meta: %{"batch_id" => batch_id}}, opts) do
    Repo.stream(conf, batch_query(batch_id), opts)
  end

  defp batch_query(batch_id) do
    Job
    |> where([j], fragment("? @> ?", j.meta, ^%{batch_id: batch_id}))
    |> where([j], not fragment("? \\? ?", j.meta, "callback"))
    |> order_by(asc: :id)
  end
end

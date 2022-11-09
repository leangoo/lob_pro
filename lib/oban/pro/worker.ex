defmodule Oban.Pro.Worker do
  @moduledoc """
  The `Oban.Pro.Worker` is a replacement for `Oban.Worker` with expanded capabilities such as
  encryption, enforced structure, output recording, and execution hooks.

  In addition, because `Batch`, `Chunk`, and `Workflow` workers are based on the Pro worker, you
  can use all of the advanced options* there as well (The one exception is that recording doesn't
  function with the `Chunk` worker).

  ## Usage

  Using `Oban.Pro.Worker` is identical to using `Oban.Worker`, with a few additional options. All
  of the basic options such as `queue`, `priority`, and `unique` are still available along with
  more advanced options.

  To create a basic Pro worker point `use` at `Oban.Pro.Worker` and define a `process/1` callback:

  ```elixir
  def MyApp.Worker do
    use Oban.Pro.Worker

    @impl Oban.Pro.Worker
    def process(%Job{} = job) do
      # Do stuff with the job
    end
  end
  ```

  If you have existing workers that you'd like to convert you only need to change the `use`
  definition and replace `perform/1` with `process/1`.

  Without any of the advanced Pro features there isn't any difference between the basic and pro
  workers.

  ## Structured Jobs

  Structured workers help you catch typos within your jobs by validating keys on insert and
  enforcing keys during execution. They also automatically generate a struct for compile-time
  checks and friendly dot access.

  #### Defining a Structured Worker

  On a structured worker the `keys` and `required` options determine which keys are allowed at
  all, and which are required. A notable benefit is that the args passed to `process/1` are
  converted into a struct:

  ```elixir
  defmodule MyApp.StructuredWorker do
    use Oban.Pro.Worker, structured: [keys: [:a, :b, :c], required: [:a, :c]]

    @impl Oban.Pro.Worker
    def process(%Job{args: %__MODULE__{a: a, c: c} = args}) do
      # Use the matched keys or access them on args
    end
  end
  ```

  The `keys` option is mandatory, but `required` is optional. If you provide a list of `required`
  keys they _must_ be a subset of the full `keys` list.

  ## Recorded Jobs

  Sometimes the output of a job is just as important as any side effects. When that's the case,
  you can use the `recorded` option to stash a job's output back into the job itself. Results are
  compressed and safely encoded for retrieval later, either manually, in a batch callback, or a in
  downstream workflow job.

  #### Defining a Recorded Worker

  ```elixir
  defmodule MyApp.RecordedWorker do
    use Oban.Pro.Worker, recorded: true

    @impl true
    def process(%Job{args: args}) do
      # Do your typical work here.
    end
  end
  ```

  If your process function returns an `{:ok, value}` tuple, it is recorded. Any other value, i.e.
  an plain `:ok`, error, or snooze, is ignored.

  The example above uses `recorded: true` to opt into recording with the defaults. That means an
  output `limit` of 32kb after compression and encoding—anything larger than the configured limit
  will return an error tuple. If you expect larger results (and you want them stored in the
  database) you can override the limit. For example, to set the limit to 64kb instead:

  ```elixir
  use Oban.Pro.Worker, recorded: [limit: 64_000]
  ```

  ### Retrieving Results

  The `fetch_recorded/1` function is your ticket to extracting recorded results. If a job has ran
  and recorded a value, it will return an `{:ok, result}` tuple:

  ```elixir
  job = MyApp.Repo.get(Oban.Job, job_id)

  case MyApp.RecordedWorker.fetch_recorded(job) do
    {:ok, result} ->
      # Use the result

    {:error, :missing} ->
      # Nothing recorded yet
  end
  ```

  ## Encrypted Jobs

  Some applications have strong regulations around the storage of personal information. For
  example, medical records, financial details, social security numbers, or other data that should
  never leak. The `encrypted` option lets you store all job data at rest with encryption so
  sensitive data can't be seen.

  #### Defining an Encrypted Worker

  Encryption is handled transparently as jobs are inserted and executed. All you need to do is
  flag the worker as encrypted and configure it to fetch a secret key:

  ```elixir
  defmodule MyApp.SensitiveWorker do
    use Oban.Pro.Worker, encrypted: [key: {module, fun, args}]

    @impl true
    def process(%Job{args: args}) do
      # Args are decrypted, use them as you normally would
    end
  end
  ```

  Now job args are encrypted before insertion into the database and decrypted when the job runs.

  #### Generating Encryption Keys

  Encryption requires a 32 byte, Base 64 encoded key. You can generate one with the `:crypto` and
  `Base` modules:

  ```elixir
  key = 32 |> :crypto.strong_rand_bytes() |> Base.encode64()
  ```

  The result will look something like this `"w7xGJClzEh1pbWuq6zsZfKfwdINu2VIkgCe3IO0hpsA="`.

  While it's possible to use the generated key in your worker directly, that defeats the purpose
  of encrypting sensitive data because anybody with access to the codebase can read the encryption
  key. That's why it is _highly_ recommended that you use an MFA to retrieve the key dynamically
  at runtime. For example, here's how you could pull the key from the Application environment:

  ```elixir
  use Oban.Pro.Worker, encrypted: [key: {Application, :fetch_key!, [:enc_key]}]
  ```

  #### Encryption Implementation Details

  * Erlang's `crypto` module is used with the `aes_256_ctr` cipher for encryption.

  * Encoding and decoding stacktraces are pruned to prevent leaking the private key or
    initialization vector.

  * Only `args` are encrypted, `meta` is kept as plaintext. You can use that to your advantage for
    uniqueness, but be careful not to put anything sensitive in `meta`.

  * Error messages and stacktraces aren't encrypted and are stored as plaintext. Be careful not to
    expose sensitive data when raising errors.

  * Args are encrypted at rest _as well as_ in Oban Web. You won't be able to view or search
    encrypted args in the Web dashboard.

  * Uniqueness works for encrypted jobs, but not for arguments because the same args are encrypted
    differently every time. Favor `meta` over `args` to enforce uniqueness for encrypted jobs.

  ## Worker Hooks

  Worker hooks are called after a job finishes executing. They can be defined as callback
  functions on the worker, or in a separate module for reuse across workers.

  Hooks are called synchronously, from within the job's process with safety applied. Any
  exceptions or crashes are caught and logged, they won't cause the job to fail or the queue to
  crash.

  Hooks _do not modify_ the job or execution results. Think of them as a convenient alternative
  to globally attached telemetry handlers. They are purely for side-effects such as cleanup,
  logging, recording metrics, broadcasting notifications, updating other records, error
  notifications, etc.

  ### Defining Hooks

  There are three mechanisms for defining and attaching an `c:after_process/2` hook:

  1. **Implicitly**—hooks are defined directly on the worker and they only run for that worker
  2. **Explicitly**—hooks are listed when defining a worker and they run anywhere they are listed
  3. **Globally**—hooks are executed for all Pro workers

  It's possible to combine each type of hook on a single worker. When multiple hooks are stacked
  they're executed in the order: implicit, explicit, and then global.

  An `c:after_process/2` hook is called with the job and an execution state corresponding to the
  result from `process/1`:

  * `complete`—when `process/1` returns `:ok` or `{:ok, result}`
  * `cancel`—when `process/1` returns `{:cancel, reason}`
  * `discard`—when a job errors and exhausts retries, or returns `{:discard, reason}`
  * `error`—when a job crashes, raises an exception, or returns `{:error, value}`
  * `snooze`—when a job returns `{:snooze, seconds}`

  First, here's how to define a single implicit local hook on the worker using
  `c:after_process/2`:

  ```elixir
  defmodule MyApp.HookWorker do
    use Oban.Pro.Worker

    @impl Oban.Pro.Worker
    def process(_job) do
      # ...
    end

    @impl Oban.Pro.Worker
    def after_process(state, %Job{} = job) do
      MyApp.Notifier.broadcast("oban-jobs", {state, %{id: job.id}})
    end
  end
  ```

  Any module that exports `c:after_process/2` can be used as a hook. For example, here we'll
  define a shared error notification hook:

  ```elixir
  defmodule MyApp.ErrorHook do
    def after_process(state, job) when state in [:discard, :error] do
      error = job.unsaved_error
      extra = Map.take([:attempt, :id, :args, :max_attempts, :meta, :queue, :worker])

      Sentry.capture_exception(error.reason, stacktrace: error.stacktrace, extra: extra)
    end

    def after_process(_state, _job), do: :ok
  end

  defmodule MyApp.HookWorker do
    use Oban.Pro.Worker, hooks: [MyApp.ErrorHook]

    @impl Oban.Pro.Worker
    def process(_job) do
      # ...
    end
  end
  ```

  The same module can be attached globally, for all `Oban.Pro.Worker` modules, using
  `attach_hook/1`:

  ```elixir
  :ok = Oban.Pro.Worker.attach_hook(MyApp.ErrorHook)
  ```
  """

  alias Oban.{Job, Worker}
  alias Oban.Pro.Stages.{Hooks, Recorded, Structured}

  @typedoc """
  Options to enable and configure `encrypted` mode.
  """
  @type encrypted :: [key: mfa()]

  @typedoc """
  Options to enable and configure `recorded` mode.
  """
  @type recorded :: true | [to: atom(), limit: pos_integer(), safe_decode: boolean()]

  @typedoc """
  Options to enable and configure `structured` mode.
  """
  @type structured :: [keys: [atom()], required: [atom()]]

  @typedoc """
  All possible hook states.
  """
  @type hook_state :: :cancel | :complete | :discard | :error | :snooze

  @doc """
  Called when executing a job.

  The `process/1` callback behaves identically to `c:Oban.Worker.perform/1`, except that it may
  have pre-processing and post-processing applied.
  """
  @callback process(job :: Job.t() | [Job.t()]) :: Worker.result()

  @doc """
  Called after a job finishes processing regardless of status (complete, failure, etc).

  See the shared "Worker Hooks" section for more details.
  """
  @callback after_process(hook_state(), job :: Job.t()) :: :ok

  @doc """
  Extract the results of a previously executed job.

  If a job has ran and recorded a value, it will return an `{:ok, result}` tuple. Otherwise,
  you'll get `{:error, :missing}`.
  """
  @callback fetch_recorded(job :: Job.t()) :: {:ok, term()} | {:error, :missing}

  @optional_callbacks after_process: 2, fetch_recorded: 1

  @doc false
  defmacro __using__(opts) do
    {stage_opts, stand_opts} = Keyword.split(opts, [:encrypted, :hooks, :recorded, :structured])

    quote location: :keep do
      @behaviour Oban.Worker
      @behaviour Oban.Pro.Worker

      unquote(stage_opts)
      |> Keyword.put_new(:standard, unquote(stand_opts))
      |> Enum.map(&Oban.Pro.Worker.init_stage!(__MODULE__, &1))
      |> Enum.each(fn {stage, conf} ->
        if stage == Structured do
          defstruct conf.keys
                    |> MapSet.to_list()
                    |> Enum.map(&String.to_existing_atom/1)
        end
      end)

      alias Oban.{Job, Worker}

      @doc false
      def __stages__ do
        unquote(stage_opts)
        |> Keyword.put_new(:standard, unquote(stand_opts))
        |> Keyword.put_new(:hooks, [])
        |> Enum.map(&Oban.Pro.Worker.init_stage!(__MODULE__, &1))
      end

      def __opts__ do
        unquote(stand_opts)
        |> Keyword.put(:worker, inspect(__MODULE__))
        |> Keyword.put(:stages, __stages__())
      end

      @impl Worker
      def new(args, opts \\ []) when is_map(args) and is_list(opts) do
        opts = Worker.merge_opts(__opts__(), opts)

        case Oban.Pro.Worker.before_new(args, opts) do
          {:ok, args, opts} ->
            Job.new(args, Keyword.delete(opts, :stages))

          {:error, message} ->
            args
            |> Job.new(Keyword.delete(opts, :stages))
            |> Ecto.Changeset.add_error(:args, message)
        end
      end

      @impl Worker
      def backoff(%Job{} = job) do
        Worker.backoff(job)
      end

      @impl Worker
      def timeout(%Job{} = job) do
        Worker.timeout(job)
      end

      @impl Worker
      def perform(%Job{} = job) do
        Oban.Pro.Worker.process(__MODULE__, job, __opts__())
      end

      @impl Oban.Pro.Worker
      def fetch_recorded(%Job{} = job) do
        conf = Keyword.fetch!(__stages__(), Recorded)

        Recorded.fetch_recorded(job, conf)
      end

      defoverridable backoff: 1, new: 2, perform: 1, timeout: 1
    end
  end

  # Global Hooks

  @doc """
  Register a worker hook to be ran after any Pro worker executes.

  The module must define a function that matches the hook. For example, a module that handles
  an `:on_complete` hook must define an `on_complete/1` function.

  ## Example

  Attach a hook handler globally:

      defmodule MyApp.Hook do
        def after_process(_state, %Oban.Job{} = job) do
          # Do something with the job

          :ok
        end
      end

      :ok = Oban.Pro.Worker.attach_hook(MyApp.Hook)
  """
  @doc since: "0.12.0"
  @spec attach_hook(module()) :: :ok | {:error, term()}
  defdelegate attach_hook(module), to: Hooks

  @doc """
  Unregister a worker hook.

  ## Example

  Detach a previously registered global hook:

      :ok = Oban.Pro.Worker.detach_hook(MyApp.Hook)
  """
  @doc since: "0.12.0"
  @spec detach_hook(module()) :: :ok
  defdelegate detach_hook(module), to: Hooks

  # Stages

  @doc false
  def init_stage!(worker, {stage_key, opts}) do
    stage_mod =
      stage_key
      |> to_string()
      |> Macro.camelize()

    stage_mod = Module.concat(Oban.Pro.Stages, stage_mod)

    case stage_mod.init(worker, opts) do
      {:ok, conf} -> {stage_mod, conf}
      {:error, reason} -> raise ArgumentError, "#{stage_key}: " <> reason
    end
  end

  @doc false
  def before_new(args, opts) do
    opts
    |> Keyword.get(:stages, [])
    |> Enum.reverse()
    |> Enum.filter(fn {stage, _} -> function_exported?(stage, :before_new, 3) end)
    |> Enum.reduce_while({:ok, args, opts}, fn {stage, conf}, {:ok, args, opts} ->
      case stage.before_new(args, opts, conf) do
        {:ok, _, _} = ok -> {:cont, ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  @doc false
  def process(module, job, opts) do
    with {:ok, job} <- before_process(job, opts) do
      job
      |> module.process()
      |> after_process(job, opts)
    end
  end

  @doc false
  def before_process(job, opts) do
    opts
    |> Keyword.get(:stages, [])
    |> Enum.filter(fn {stage, _} -> function_exported?(stage, :before_process, 2) end)
    |> Enum.reduce_while({:ok, job}, fn {stage, conf}, {:ok, job} ->
      case stage.before_process(job, conf) do
        {:ok, _} = ok -> {:cont, ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  @doc false
  def after_process(result, job, opts) do
    opts
    |> Keyword.get(:stages, [])
    |> Enum.filter(fn {stage, _} -> function_exported?(stage, :after_process, 3) end)
    |> Enum.reduce_while(result, fn {stage, conf}, result ->
      case stage.after_process(result, job, conf) do
        :ok -> {:cont, result}
        other -> {:halt, other}
      end
    end)
  end

  # Telemetry

  @doc false
  def on_start do
    events = [[:oban, :job, :stop], [:oban, :job, :exception]]

    :telemetry.attach_many("oban.pro.worker", events, &Hooks.handle_event/4, nil)
  end

  @doc false
  def on_stop do
    :telemetry.detach("oban.pro.worker")
  end
end

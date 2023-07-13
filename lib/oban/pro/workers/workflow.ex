defmodule Oban.Pro.Workers.Workflow do
  @moduledoc ~S"""
  Workflow workers compose together with arbitrary dependencies between jobs,
  allowing sequential, fan-out, and fan-in execution workflows. Workflows are fault
  tolerant, can be homogeneous or heterogeneous, and scale horizontally across all
  available nodes.

  Workflow jobs aren't executed until all upstream dependencies have completed. This
  includes waiting on retries, scheduled jobs, or snoozing.

  ## Usage

  Each worker within a workflow **must use** `Oban.Pro.Workers.Workflow` rather than
  `Oban.Worker`, otherwise jobs will execute without any dependency management.

  Our example workflow will coordinate archiving an account. To properly archive
  the account we must perform the following workers:

  1. `FinalReceipt` — generate a final receipt and email it to the account owner
  2. `EmailSubscriber` — email each subscriber on the account
  3. `BackupPost` —  up all important posts on the account to external storage
  4. `DeleteAccount` — delete the account from the database

  We need step 1 to run before step 2 or 3, but both steps 2 and 3 can run in
  parallel. After all of the jobs in steps 2 and 3 finish we can finally delete
  the account from the database. For demonstration purposes we'll define one of
  the workers with a stubbed-in `process/1` function:

  ```elixir
  defmodule MyApp.FinalReceipt do
    use Oban.Pro.Workers.Workflow, queue: :default

    @impl true
    def process(%Job{} = _job) do
      # Generate the final receipt.

      :ok
    end
  end
  ```

  Note that we define a **`process/1` callback instead of `perform/1`** because
  the `perform/1` function coordinates the workflow. The `process/1` function
  receives an `Oban.Job` struct, just like `perform/1` and it should return
  similar values, i.e. `:ok`, `{:ok, value}`.

  Assuming that you've defined the other workers, we can construct a workflow to
  enforce sequential execution:

  ```elixir
  alias MyApp.{BackupPost, DeleteAccount, EmailSubscriber, FinalReceipt}

  def archive_account(acc_id) do
    FinalReceipt.new_workflow()
    |> FinalReceipt.add(:receipt, FinalReceipt.new(%{id: acc_id}))
    |> FinalReceipt.add(:email, EmailSubscriber.new(%{id: acc_id}), deps: [:receipt])
    |> FinalReceipt.add(:backup, BackupPost.new(%{id: acc_id}), deps: [:receipt])
    |> FinalReceipt.add(:delete, DeleteAccount.new(%{id: acc_id}), deps: [:backup, :receipt])
    |> Oban.insert_all()
  end
  ```

  Here we use `c:new_workflow/1` to initialize a new workflow (later on we will
  look at how to customize workflow behaviour with various options). Jobs are then
  added to the workflow with `add/3,4`, where the first argument is an existing
  workflow, the second argument is unique name, the third is a job changeset, and
  the fourth an optional list of dependencies. Finally, the workflow passes
  directly to `Oban.insert_all/1,2`, which handles inserting each of the jobs into
  the database atomically.

  _All workers that `use Workflow` have `c:new_workflow/1` and `c:add/4`
  functions defined, so it doesn't matter which module you use to initialize or
  to add new jobs._

  Dependency resolution guarantees that jobs execute in the order `receipt ->
  email -> backup -> delete`, even if one of the jobs fails and needs to retry.
  Any job that defines a dependency will wait for each upstream dependency to
  complete before it starts.

  Recall how our original specification stated that there could be multiple
  `email` and `backup` jobs? The workflow we've built only handles one email or
  backup job at a time.

  Let's modify the workflow to fan-out to multiple `email`
  and `backup` jobs and then fan-in to the final `delete` job:

  ```elixir
  def archive_account_workflow(acc_id) do
    subscribers = subscribers_for_account(acc_id)
    documents = documents_for_account(acc_id)

    email_deps = Enum.map(subscribers, &"email_#{&1.id}")
    backup_deps = Enum.map(documents, &"backup_#{&1.id}")
    delete_deps = email_deps ++ backup_deps

    FinalReceipt.new_workflow()
    |> FinalReceipt.add(:receipt, FinalReceipt.new(%{id: acc_id}))
    |> add_email_jobs(subscribers)
    |> add_backup_jobs(documents)
    |> DeleteAccount.add(:delete, DeleteAccount.new(%{id: acc_id}), deps: delete_deps)
  end

  defp add_email_jobs(workflow, subscribers) do
    Enum.reduce(subscribers, workflow, fn %{id: id, email: email}, acc ->
      EmailSubscriber.add(acc, "email_#{id}", EmailSubscriber.new(%{email: email}), deps: [:receipt])
    end)
  end

  defp add_backup_jobs(workflow, subscribers) do
    Enum.reduce(subscribers, workflow, fn %{id: id}, acc ->
      BackupPost.add(acc, "backup_#{id}", BackupPost.new(%{id: id}), deps: [:receipt])
    end)
  end
  ```

  Now the workflow will run all of the `email` and `backup` jobs we need, _before_
  deleting the account. To confirm the flow without inserting and executing jobs
  we can visualize it.

  ## Creating Workflows

  Workflows use conservative defaults for safe, and relatively quick, dependency
  resolution. You can customize the safety checks, waiting times and resolution
  intensity by providing a few top-level options:

  * `ignore_cancelled` — regard `cancelled` dependencies as completed rather than
    halting the workflow. Defaults to `false`.

  * `ignore_discarded` — regard `discarded` dependencies as completed rather than
    halting the workflow. Defaults to `false`.

  * `ignore_deleted` — regard deleted (typically pruned) dependencies as completed
    rather than halting the workflow. Defaults to `false`.

  * `waiting_delay` — the number of milliseconds to wait between dependency
    checks. This value, combined with the `waiting_limit`, dictates how long a
    job will await upstream dependencies. Defaults to `1000ms`.

  * `waiting_limit` — the number of times to retry dependency checks. This value,
    combined with `waiting_delay`, dictates how long a job will await upstream
    dependencies. Defaults to `10`.

  * `waiting_snooze` — the number of seconds a job will snooze after awaiting
    **executing** upstream dependencies. If upstream dependencies are `scheduled`
    or `retryable` then the job snoozes until the latest dependency's
    `scheduled_at` timestamp.

  The following example creates a workflow with all of the available options:

  ```elixir
  alias Oban.Pro.Workers.Workflow

  workflow = Workflow.new(
    ignore_cancelled: true,
    ignore_deleted: true,
    ignore_discarded: true,
    waiting_delay: :timer.seconds(5),
    waiting_limit: 5,
    waiting_snooze: 10
  )
  ```

  Internally, the `meta` field stores options for each job. That makes it possible
  to set or override workflow options per-job. For example, configure a single job
  to ignore cancelled dependencies and another to snooze longer:

  ```elixir
  MyWorkflow.new_workflow()
  |> MyWorkflow.add(:a, MyWorkflow.new(%{}))
  |> MyWorkflow.add(:b, MyWorkflow.new(%{}, meta: %{ignore_cancelled: true}), deps: [:a])
  |> MyWorkflow.add(:c, MyWorkflow.new(%{}, meta: %{waiting_snooze: 30}), deps: [:b])
  ```

  Dependency resolution relies on jobs lingering in the database after
  execution. If your system prunes job dependencies then the workflow may never
  complete. To override this behaviour, set `ignore_deleted: true` on your
  workflows.

  ## Generating Workflow IDs

  By default `workflow_id` is a version 4 random [UUID][uuid]. This is more than
  sufficient to ensure that workflows are unique for any period of time. However,
  if you require more control you can override `workflow_id` generation at the
  worker level, or pass a value directly to the `c:new_workflow/1` function.

  To override the `workflow_id` for a particular workflow you override the `c:gen_id/0`
  callback:

  ```elixir
  defmodule MyApp.Workflow do
    use Oban.Pro.Workers.Workflow

    # Generate a 24 character long random string instead
    @impl true
    def gen_id do
      24
      |> :crypto.strong_rand_bytes()
      |> Base.encode64()
    end
    ...
  end
  ```

  The `c:gen_id/0` callback works for random/non-deterministic id generation. If
  you'd prefer to use a deterministic id instead you can pass the `workflow_id` in
  as an option to `c:new_workflow/1`:

  ```elixir
  MyApp.Workflow.new_workflow(workflow_id: "custom-id")
  ```

  Using this technique you can verify the `workflow_id` in tests or append to the
  workflow manually after it was originally created.

  [uuid]: https://en.wikipedia.org/wiki/Universally_unique_identifier

  ## Appending to a Workflow

  Sometimes all jobs aren't known when the workflow is created. In that case, you
  can add more jobs with optional dependency checking using `append_workflow/2`.
  An appended workflow starts with one or more jobs, which reuses the original
  workflow id, and optionally builds a set of dependencies for checking.

  In this example we disable deps checking with `check_deps: false`:

  ```elixir
  defmodule MyApp.WorkflowWorker do
    use Oban.Pro.Workers.Workflow

    @impl true
    def process(%Job{} = job) do
      jobs =
        job
        |> append_workflow(check_deps: false)
        |> add(:d, WorkerD.new(%{}), deps: [:a])
        |> add(:e, WorkerE.new(%{}), deps: [:b])
        |> add(:f, WorkerF.new(%{}), deps: [:c])
        |> Oban.insert_all()

      {:ok, jobs}
    end
  end
  ```

  The new jobs specify deps on preexisting jobs named `:a`, `:b`, and `:c`, but
  there isn't any guarantee those jobs actually exist. That could lead to an
  incomplete workflow where the new jobs may never complete.

  To be safe and check jobs while appending we'll fetch all of the previous jobs
  and feed them in:

  ```elixir
  defmodule MyApp.WorkflowWorker do
    use Oban.Pro.Workers.Workflow

    @impl true
    def process(%Job{} = job) do
      {:ok, jobs} =
        MyApp.Repo.transaction(fn ->
          job
          |> stream_workflow_jobs()
          |> Enum.to_list()
        end)

      jobs
      |> append_workflow()
      |> add(:d, WorkerD.new(%{}), deps: [:a])
      |> add(:e, WorkerE.new(%{}), deps: [:b])
      |> add(:f, WorkerF.new(%{}), deps: [:c])
      |> Oban.insert_all()

      :ok
    end
  end
  ```

  Now there isn't any risk of an incomplete workflow, at the expense of loading
  some extraneous jobs.

  ## Fetching Workflow Jobs

  The `c:all_workflow_jobs/1,2` function simplifies loading all jobs in a workflow from within a
  worker. For example, to fetch all of the jobs in a workflow:

  ```elixir
  defmodule MyApp.Workflow do
    use Oban.Pro.Workers.Workflow

    @impl Workflow
    def process(%Job{} = job) do
      job
      |> all_workflow_jobs()
      |> do_things_with_jobs()

      :ok
    end
  end
  ```

  It's also possible to scope fetching to only dependencies of the current job:

  ```elixir
  deps = all_workflow_jobs(job, only_deps: true)
  ```

  Or only a single explicit dependency:

  ```elixir
  [dep_job] = all_workflow_jobs(job, names: [:a])
  ```

  For large workflows it's not efficient to load all jobs in memory at once. In
  that case, you can the `c:stream_workflow_jobs/1,2` callback instead to fetch
  jobs lazily. For example, to stream all of the `completed` jobs in a workflow:

  ```elixir
  defmodule MyApp.Workflow do
    use Oban.Pro.Workers.Workflow

    @impl Workflow
    def process(%Job{} = job) do
      {:ok, workflow_jobs} =
        MyApp.Repo.transaction(fn ->
          job
          |> stream_workflow_jobs()
          |> Stream.filter(& &1.state == "completed")
          |> Enum.to_list()
        end)

      do_things_with_jobs(workflow_jobs)

      :ok
    end
  end
  ```

  Streaming is provided by Ecto's `Repo.stream`, and it must take place within a
  transaction. Using a stream lets you control the number of jobs loaded from the
  database, minimizing memory usage for large workflows.

  ## Visualizing Workflows

  Workflows are a type of [Directed Acyclic Graph][dag], also known as a DAG. That
  means we can describe a workflow as a graph of jobs and dependencies, where
  execution flows between jobs. By converting the workflow into [DOT][dot]
  notation, a standard graph description language, we can render visualizations!

  Dot generation relies on [libgraph][libgraph], which is an optional dependency.
  You'll need to specify it as a dependency before generating dot output:

  ```elixir
  def deps do
    [{:libgraph, "~> 0.7"}]
  end
  ```

  Once you've installed `libgraph`, we can use `to_dot/1` to convert a workflow.
  As with `new_workflow` and `add`, all workflow workers define a `to_dot/1`
  function that takes a workflow and returns a dot formatted string. For example,
  calling `to_dot/1` with the account archiving workflow from above:

  ```elixir
  FinalReceipt.to_dot(archive_account_workflow(123))
  ```

  Generates the following dot output, where each vertex is a combination of the
  job's name in the workflow and its worker module:

  ```text
  strict digraph {
      "delete (MyApp.DeleteAccount)"
      "backup_1 (MyApp.BackupPost)"
      "backup_2 (MyApp.BackupPost)"
      "backup_3 (MyApp.BackupPost)"
      "receipt (MyApp.FinalReceipt)"
      "email_1 (MyApp.EmailSubscriber)"
      "email_2 (MyApp.EmailSubscriber)"
      "backup_1 (MyApp.BackupPost)" -> "delete (MyApp.DeleteAccount)" [weight=1]
      "backup_2 (MyApp.BackupPost)" -> "delete (MyApp.DeleteAccount)" [weight=1]
      "backup_3 (MyApp.BackupPost)" -> "delete (MyApp.DeleteAccount)" [weight=1]
      "receipt (MyApp.FinalReceipt)" -> "backup_1 (MyApp.BackupPost)" [weight=1]
      "receipt (MyApp.FinalReceipt)" -> "backup_2 (MyApp.BackupPost)" [weight=1]
      "receipt (MyApp.FinalReceipt)" -> "backup_3 (MyApp.BackupPost)" [weight=1]
      "receipt (MyApp.FinalReceipt)" -> "email_1 (MyApp.EmailSubscriber)" [weight=1]
      "receipt (MyApp.FinalReceipt)" -> "email_2 (MyApp.EmailSubscriber)" [weight=1]
      "email_1 (MyApp.EmailSubscriber)" -> "delete (MyApp.DeleteAccount)" [weight=1]
      "email_2 (MyApp.EmailSubscriber)" -> "delete (MyApp.DeleteAccount)" [weight=1]
  }
  ```

  Now we can take that dot output and render it using a tool like [graphviz][gv].
  The following example function accepts a workflow and renders it out as an SVG:

  ```elixir
  defmodule WorkflowRenderer do
    alias Oban.Pro.Workers.Workflow

    def render(workflow) do
      dot_path = "workflow.dot"
      svg_path = "workflow.svg"

      File.write!(dot_path, Workflow.to_dot(workflow))

      System.cmd("dot", ["-T", "svg", "-o", svg_path, dot_path])
    end
  end
  ```

  With [graphviz][gv] installed, that will generate a SVG of the workflow:

  <svg viewBox="0 0 1381.58 188" xmlns="http://www.w3.org/2000/svg"><g class="graph"><path fill="#fff" stroke="transparent" d="M0 188V0h1381.58v188H0z"/><g class="node" transform="translate(4 184)"><ellipse fill="none" stroke="#000" cx="663.44" cy="-18" rx="122.68" ry="18"/><text text-anchor="middle" x="663.44" y="-14.3" font-family="Times,serif" font-size="14">delete (MyApp.DeleteAccount)</text></g><g class="node" transform="translate(4 184)"><ellipse fill="none" stroke="#000" cx="125.44" cy="-90" rx="125.38" ry="18"/><text text-anchor="middle" x="125.44" y="-86.3" font-family="Times,serif" font-size="14">backup_1 (MyApp.BackupPost)</text></g><g class="edge" transform="translate(4 184)"><path fill="none" stroke="#000" d="M214.88-77.36c96.49 12.55 249.69 32.48 349.69 45.5"/><path stroke="#000" d="M565.21-35.31l9.47 4.76-10.37 2.18.9-6.94z"/></g><g class="node" transform="translate(4 184)"><ellipse fill="none" stroke="#000" cx="394.44" cy="-90" rx="125.38" ry="18"/><text text-anchor="middle" x="394.44" y="-86.3" font-family="Times,serif" font-size="14">backup_2 (MyApp.BackupPost)</text></g><g class="edge" transform="translate(4 184)"><path fill="none" stroke="#000" d="M452.15-73.98C494.34-63 551.7-48.08 596.01-36.55"/><path stroke="#000" d="M597.03-39.9l8.8 5.91-10.56.87 1.76-6.78z"/></g><g class="node" transform="translate(4 184)"><ellipse fill="none" stroke="#000" cx="663.44" cy="-90" rx="125.38" ry="18"/><text text-anchor="middle" x="663.44" y="-86.3" font-family="Times,serif" font-size="14">backup_3 (MyApp.BackupPost)</text></g><g class="edge" transform="translate(4 184)"><path fill="none" stroke="#000" d="M663.44-71.7v25.59"/><path stroke="#000" d="M666.94-46.1l-3.5 10-3.5-10h7z"/></g><g class="node" transform="translate(4 184)"><ellipse fill="none" stroke="#000" cx="663.44" cy="-162" rx="118.08" ry="18"/><text text-anchor="middle" x="663.44" y="-158.3" font-family="Times,serif" font-size="14">receipt (MyApp.FinalReceipt)</text></g><g class="edge" transform="translate(4 184)"><path fill="none" stroke="#000" d="M576.7-149.71c-95.97 12.48-250.38 32.57-351.35 45.71"/><path stroke="#000" d="M225.51-100.49l-10.36-2.18 9.46-4.76.9 6.94z"/></g><g class="edge" transform="translate(4 184)"><path fill="none" stroke="#000" d="M606.39-146.15c-42.15 10.96-99.7 25.94-144.2 37.52"/><path stroke="#000" d="M462.89-105.2l-10.56-.87 8.8-5.9 1.76 6.77z"/></g><g class="edge" transform="translate(4 184)"><path fill="none" stroke="#000" d="M663.44-143.7v25.59"/><path stroke="#000" d="M666.94-118.1l-3.5 10-3.5-10h7z"/></g><g class="node" transform="translate(4 184)"><ellipse fill="none" stroke="#000" cx="944.44" cy="-90" rx="137.28" ry="18"/><text text-anchor="middle" x="944.44" y="-86.3" font-family="Times,serif" font-size="14">email_1 (MyApp.EmailSubscriber)</text></g><g class="edge" transform="translate(4 184)"><path fill="none" stroke="#000" d="M722.35-146.33c44.17 11.01 104.81 26.11 151.56 37.76"/><path stroke="#000" d="M874.81-111.95l8.86 5.81-10.55.98 1.69-6.79z"/></g><g class="node" transform="translate(4 184)"><ellipse fill="none" stroke="#000" cx="1236.44" cy="-90" rx="137.28" ry="18"/><text text-anchor="middle" x="1236.44" y="-86.3" font-family="Times,serif" font-size="14">email_2 (MyApp.EmailSubscriber)</text></g><g class="edge" transform="translate(4 184)"><path fill="none" stroke="#000" d="M752.66-150.1c101.88 12.45 268.46 32.8 377.11 46.07"/><path stroke="#000" d="M1130.27-107.5l9.5 4.69-10.35 2.26.85-6.95z"/></g><g class="edge" transform="translate(4 184)"><path fill="none" stroke="#000" d="M883.47-73.81c-44.37 11.05-104.5 26.03-150.69 37.54"/><path stroke="#000" d="M733.4-32.82l-10.55-.98 8.85-5.81 1.7 6.79z"/></g><g class="edge" transform="translate(4 184)"><path fill="none" stroke="#000" d="M1139.89-77.2L764.77-31.38"/><path stroke="#000" d="M765.05-27.89l-10.35-2.26 9.5-4.69.85 6.95z"/></g></g></svg>

  Looking at the visualized graph we can clearly see how the workflow starts with
  a single `render` job, fans-out to multiple `email` and `backup` jobs, and
  finally fans-in to the `delete` job—exactly as we planned!

  [dag]: https://en.wikipedia.org/wiki/Directed_acyclic_graph
  [dot]: https://en.wikipedia.org/wiki/DOT_%28graph_description_language%29
  [libgraph]: https://github.com/bitwalker/libgraph
  [gv]: https://graphviz.org
  """

  import Ecto.Query, only: [group_by: 3, order_by: 2, select: 3, where: 3]

  alias Ecto.Changeset
  alias Oban.{Job, Repo, Worker}

  @type name :: atom() | String.t()
  @type chan :: Job.changeset()

  @type add_option :: {:deps, [name()]}

  @type new_option ::
          {:ignore_cancelled, boolean()}
          | {:ignore_deleted, boolean()}
          | {:ignore_discarded, boolean()}
          | {:waiting_delay, pos_integer()}
          | {:waiting_limit, pos_integer()}
          | {:waiting_snooze, pos_integer()}
          | {:workflow_id, String.t()}

  @type append_option :: new_option() | {:check_deps, boolean()}

  @type fetch_opts ::
          {:log, Logger.level()}
          | {:names, [atom()]}
          | {:only_deps, boolean()}
          | {:timeout, timeout()}

  @type t :: %__MODULE__{
          id: String.t(),
          changesets: [chan()],
          check_deps: boolean(),
          names: MapSet.t(),
          opts: map()
        }

  @doc """
  Instantiate a new workflow struct with a unique workflow id.

  ## Examples

  Create a simple workflow without any options:

      MyApp.MyWorkflow.new_workflow()
      |> MyWorkflow.add(:a, MyApp.WorkerA.new(%{}))
      |> MyWorkflow.add(:b, MyApp.WorkerB.new(%{}))

  Create a workflow with a static id and some options:

      MyWorkflow.new_workflow(workflow_id: "workflow-id", ignore_cancelled: true)
  """
  @callback new_workflow(opts :: [new_option()]) :: t()

  @doc """
  Instantiate a new workflow from an existing workflow job, jobs, or a stream.

  ## Examples

  Append to a workflow seeded with all other jobs in the workflow:

      jobs
      |> append_workflow()
      |> add(:d, WorkerD.new(%{}), deps: [:a])
      |> add(:e, WorkerE.new(%{}), deps: [:b])
      |> Oban.insert_all()

  Append to a workflow from a single job and bypass checking deps:

      job
      |> append_workflow(check_deps: false)
      |> add(:d, WorkerD.new(%{}), deps: [:a])
      |> add(:e, WorkerE.new(%{}), deps: [:b])
      |> Oban.insert_all()
  """
  @callback append_workflow(jobs :: Job.t() | [Job.t()], [append_option()]) :: t()

  @doc """
  Add a named job to the workflow along with optional dependencies.

  ## Examples

  Add jobs to a workflow with dependencies:

      MyApp.MyWorkflow.new_workflow()
      |> MyApp.MyWorkflow.add(:a, MyApp.WorkerA.new(%{id: id}))
      |> MyApp.MyWorkflow.add(:b, MyApp.WorkerB.new(%{id: id}), deps: [:a])
      |> MyApp.MyWorkflow.add(:c, MyApp.WorkerC.new(%{id: id}), deps: [:a])
      |> MyApp.MyWorkflow.add(:d, MyApp.WorkerC.new(%{id: id}), deps: [:b, :c])
  """
  @callback add(flow :: t(), name :: name(), changeset :: chan(), opts :: [add_option()]) :: t()

  @doc """
  Generate a unique string to identify the workflow.

  Defaults to a 128bit UUID.

  ## Examples

  Generate a workflow id using random bytes instead of a UUID:

      @impl Workflow
      def gen_id do
        24
        |> :crypto.strong_rand_bytes()
        |> Base.encode64()
      end
  """
  @callback gen_id() :: String.t()

  @doc """
  Converts the given workflow to DOT format, which can then be converted to a number of other
  formats via Graphviz, e.g. `dot -Tpng out.dot > out.png`.

  The default implementation relies on [libgraph](https://hexdocs.pm/libgraph/Graph.html).

  ## Examples

  Generate a DOT graph format from a workflow:

      MyApp.MyWorkflow.to_dot(workflow)
  """
  @callback to_dot(flow :: t()) :: String.t()

  @doc """
  Get all jobs for a workflow, optionally filtered by upstream deps.

  ## Examples

  Retrieve all workflow jobs:

      @impl Workflow
      def process(%Job{} = job) do
        job
        |> all_workflow_jobs()
        |> do_things_with_jobs()

        :ok
      end

  Retrieve only current job's deps:

      workflow_jobs = all_workflow_jobs(job, only_deps: true)

  Retrieve an explicit list of dependencies:

      [job_a, job_b] = all_workflow_jobs(job, names: [:a, :b])
  """
  @callback all_workflow_jobs(Job.t(), [fetch_opts()]) :: [Job.t()]

  @doc """
  Stream all jobs for a workflow.

  ## Examples

  Stream with filtering to only preserve `completed` jobs:

    @impl Workflow
    def process(%Job{} = job) do
      {:ok, workflow_jobs} =
        MyApp.Repo.transaction(fn ->
          job
          |> stream_workflow_jobs()
          |> Stream.filter(& &1.state == "completed")
          |> Enum.to_list()
        end)

      do_things_with_jobs(workflow_jobs)

      :ok
    end
  """
  @callback stream_workflow_jobs(Job.t(), [fetch_opts()]) :: Enum.t()

  defstruct [:id, changesets: [], check_deps: true, names: MapSet.new(), opts: %{}]

  defmacro __using__(opts) do
    quote location: :keep do
      use Oban.Pro.Worker, unquote(opts)

      alias Oban.Pro.Workers.Workflow

      @behaviour Workflow

      defguardp is_name(name) when (is_atom(name) and not is_nil(name)) or is_binary(name)

      @impl Workflow
      def new_workflow(opts \\ []) when is_list(opts) do
        opts
        |> Keyword.put_new(:workflow_id, gen_id())
        |> Workflow.new()
      end

      @impl Workflow
      def append_workflow(jobs, opts \\ []) do
        Workflow.append(jobs, opts)
      end

      @impl Workflow
      def add(%_{} = workflow, name, %Changeset{} = changeset, opts \\ []) when is_name(name) do
        Workflow.add(workflow, name, changeset, opts)
      end

      @impl Workflow
      def gen_id do
        Workflow.gen_id()
      end

      @impl Workflow
      def to_dot(workflow) do
        Workflow.to_dot(workflow)
      end

      @impl Workflow
      def stream_workflow_jobs(%Job{} = job, opts \\ []) do
        Workflow.stream_jobs(job, opts)
      end

      @impl Workflow
      def all_workflow_jobs(%Job{} = job, opts \\ []) do
        Workflow.all_jobs(job, opts)
      end

      @impl Worker
      def perform(%Job{} = job) do
        opts = __opts__()

        with {:ok, job} <- Oban.Pro.Worker.before_process(job, opts) do
          job
          |> Workflow.maybe_process(__MODULE__)
          |> Oban.Pro.Worker.after_process(job, opts)
        end
      end

      defoverridable Workflow
    end
  end

  # Public Interface

  @doc """
  Initialize a new workflow.

  """
  @spec new(opts :: [new_option()]) :: t()
  def new(opts \\ []) do
    opts =
      opts
      |> Keyword.put_new_lazy(:workflow_id, &gen_id/0)
      |> Map.new()

    %__MODULE__{id: opts.workflow_id, opts: opts}
  end

  @doc false
  def append([%Job{meta: %{"workflow_id" => id}} | _] = jobs, opts) do
    {check, opts} = Keyword.pop(opts, :check_deps, true)

    workflow =
      opts
      |> Keyword.put(:workflow_id, id)
      |> new()

    %{workflow | check_deps: check, names: MapSet.new(jobs, & &1.meta["name"])}
  end

  def append(%Job{} = job, opts), do: append([job], opts)

  @doc false
  def add(%_{} = workflow, name, %Changeset{} = changeset, opts) do
    name = to_string(name)

    deps =
      opts
      |> Keyword.get(:deps, [])
      |> Enum.map(&to_string/1)

    prevent_dupe!(workflow, name)
    ensure_workflow!(name, changeset)

    if workflow.check_deps, do: confirm_deps!(workflow, deps)

    meta =
      changeset
      |> Changeset.get_change(:meta, %{})
      |> Map.put(:deps, deps)
      |> Map.put(:name, name)
      |> Map.put(:workflow_id, workflow.id)
      |> Map.merge(workflow.opts)

    changesets = workflow.changesets ++ [Changeset.put_change(changeset, :meta, meta)]

    %{workflow | changesets: changesets, names: MapSet.put(workflow.names, name)}
  end

  @doc false
  def gen_id do
    Ecto.UUID.generate()
  end

  @doc false
  def to_dot(%__MODULE__{changesets: changesets}) do
    {:ok, dot} =
      changesets
      |> Enum.map(&{&1.changes.meta.name, &1.changes.worker, &1.changes.meta.deps})
      |> Enum.reduce(Graph.new(), fn {name, worker, deps}, graph ->
        label = "#{name} (#{worker})"

        graph
        |> Graph.add_vertex(name, label)
        |> Graph.add_edges(for dep <- deps, do: {dep, name})
      end)
      |> Graph.to_dot()

    dot
  end

  @doc false
  def all_jobs(%Job{conf: conf, meta: %{"deps" => deps, "workflow_id" => workflow_id}}, opts) do
    {query_opts, opts} = Keyword.split(opts, [:names, :only_deps])

    Repo.all(conf, workflow_query(workflow_id, deps, query_opts), opts)
  end

  @doc false
  def stream_jobs(%Job{conf: conf, meta: %{"deps" => deps, "workflow_id" => workflow_id}}, opts) do
    {query_opts, opts} = Keyword.split(opts, [:names, :only_deps])

    Repo.stream(conf, workflow_query(workflow_id, deps, query_opts), opts)
  end

  @doc false
  def maybe_process(job, module, waiting_count \\ 0) do
    meta = meta_with_defaults(job.meta)

    case check_deps(job.conf, job) do
      :completed ->
        module.process(job)

      :available ->
        {:snooze, meta.waiting_snooze}

      :executing ->
        if waiting_count >= meta.waiting_limit do
          {:snooze, meta.waiting_snooze}
        else
          Process.sleep(meta.waiting_delay)

          maybe_process(job, module, waiting_count + 1)
        end

      {:scheduled, scheduled_at} ->
        seconds =
          scheduled_at
          |> DateTime.diff(DateTime.utc_now())
          |> max(meta.waiting_snooze)

        {:snooze, seconds}

      :cancelled ->
        if meta.ignore_cancelled do
          module.process(job)
        else
          {:cancel, "upstream deps cancelled, workflow will never complete"}
        end

      :discarded ->
        if meta.ignore_discarded do
          module.process(job)
        else
          {:cancel, "upstream deps discarded, workflow will never complete"}
        end

      :deleted ->
        if meta.ignore_deleted do
          module.process(job)
        else
          {:cancel, "upstream deps deleted, workflow will never complete"}
        end
    end
  end

  # Helpers

  @default_meta %{
    ignore_cancelled: false,
    ignore_deleted: false,
    ignore_discarded: false,
    waiting_delay: :timer.seconds(1),
    waiting_limit: 10,
    waiting_snooze: 5
  }

  defp meta_with_defaults(meta) do
    for {key, val} <- @default_meta, into: %{}, do: {key, meta[to_string(key)] || val}
  end

  defp check_deps(conf, %{meta: %{"deps" => [_ | _]}} = job) do
    %{"deps" => deps, "workflow_id" => workflow_id} = job.meta

    deps_count = length(deps)

    query =
      Job
      |> where([j], fragment("? @> ?", j.meta, ^%{workflow_id: workflow_id}))
      |> where([j], fragment("?->>'name'", j.meta) in ^deps)
      |> group_by([j], j.state)
      |> select([j], {j.state, count(j.id), max(j.scheduled_at)})

    conf
    |> Repo.all(query)
    |> Map.new(fn {state, count, sc_at} -> {state, {count, sc_at}} end)
    |> case do
      %{"completed" => {^deps_count, _}} -> :completed
      %{"scheduled" => {_, scheduled_at}} -> {:scheduled, scheduled_at}
      %{"retryable" => {_, scheduled_at}} -> {:scheduled, scheduled_at}
      %{"executing" => _} -> :executing
      %{"available" => _} -> :available
      %{"cancelled" => _} -> :cancelled
      %{"discarded" => _} -> :discarded
      %{} -> :deleted
    end
  end

  defp check_deps(_conf, _job) do
    :completed
  end

  defp prevent_dupe!(workflow, name) do
    if MapSet.member?(workflow.names, name) do
      raise "#{inspect(name)} is already a member of the workflow"
    end
  end

  defp ensure_workflow!(name, changeset) do
    with {:ok, worker} <- Changeset.fetch_change(changeset, :worker),
         {:ok, worker} <- Worker.from_string(worker) do
      unless function_exported?(worker, :new_workflow, 1) do
        raise "#{inspect(name)} does not implement the Workflow behaviour"
      end
    end
  end

  defp confirm_deps!(_workflow, []), do: :ok

  defp confirm_deps!(workflow, deps) do
    missing = for name <- deps, not MapSet.member?(workflow.names, name), do: name

    unless Enum.empty?(missing) do
      raise "deps #{inspect(missing)} are not members of the workflow"
    end
  end

  # Query Helpers

  defp workflow_query(workflow_id, deps, opts) do
    Job
    |> where([j], fragment("? @> ?", j.meta, ^%{workflow_id: workflow_id}))
    |> order_by(asc: :id)
    |> scope_to_deps(deps, opts)
  end

  defp scope_to_deps(query, deps, opts) do
    cond do
      is_list(opts[:names]) ->
        names = Enum.map(opts[:names], &to_string/1)

        where(query, [j], j.meta["name"] in ^names)

      opts[:only_deps] ->
        where(query, [j], j.meta["name"] in ^deps)

      true ->
        query
    end
  end
end

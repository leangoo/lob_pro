defmodule Oban.Pro.Plugins.DynamicScaler do
  @moduledoc """
  The `DynamicScaler` examines queue throughput and issues commands to horizontally scale
  cloud infrastructure to optimize processing. With auto-scaling you can spin up additional nodes
  during high traffic events, and pare down to a single node during a lull. Beyond optimizing
  throughput, scaling may save money in environments with little to no usage at off-peak times,
  e.g. staging.

  Horizontal scaling is applied at the _node_ level, not the _queue_ level, so you can distribute
  processing over more phyiscal hardware.

  * Predictive Scaling — The optimal scale is calculated by predicting the future size of a queue
    based on recent trends. Multiple samples are then used to prevent overreacting to changes in
    queue depth or throughput. Your provide an acceptible `range` of nodes and auto-scaling takes
    care of the rest.

  * Multi-Cloud — Cloud integration is provided by a simple, flexible, behaviour that you
    implement for your specific environment and configure for each scaler.

  * Queue Filtering — By default, all queues are considered for scale calculations. However, you
    can restrict calculations to one or more ciritical queues.

  * Multiple Scalers — Some systems may restrict work to specific node types, e.g. generating
    exports or processing videos. Other hybrid systems may straddle multiple clouds. In either
    case, you can configure multiple independent scalers driven by distinct queues.

  * Non Linear — An optional `step` parameter allows you to conservatively scale up or down one
    node at a time, or optimize for responsiveness and jump from the min to the max in a single
    scaling period.

  * Prevent Thrashing — A `cooldown` period skips scaling when there was recent scale activity to
    prevent unnecessarily scaling nodes up or down. Nodes may take several minutes to start within
    an environment, so the default `cooldown` period is 2 minutes.

  ## Using and Configuring

  > #### Clouds {: .info}
  >
  > There are ample hosting platforms, aka "clouds", out there and we can't support them all!
  > Before you can begin dynamic scaling you'll need to implement a `Cloud` module for your
  > environment. Don't worry, we have [copy-and-paste examples](#module-cloud-modules) for some
  > popular platforms and a guide to walk through [implementations for your
  > environment](#module-writing-cloud-modules).

  With a `cloud` module in hand you're ready to add the `DynamicScaler` plugin to your Oban
  config:

      config :my_app, Oban,
        plugins: [
          {Oban.Pro.Plugins.DynamicScaler, ...}
        ]

  Then, add a scaler with a `range` to define the minimum and maximum nodes, and your `cloud`
  strategy:

      {DynamicScaler, scalers: [range: 1..5, cloud: MyApp.Cloud]}

  Now, every minute, `DynamicScaler` will calculate the optimal number of nodes for your queue's
  throughput and issue scaling commands accordingly.

  ### Configuring Scalers

  Scalers have options beyond `:cloud` and `:range` for more advanced systems or to constrain
  resource usage. Here's a breakdown of all options, followed by specific examples of each.

  * `:cloud` — A `module` or `{module, options}` tuple that interacts with the external cloud
    during scale events. Required.

  * `:range` — The range of compute units to scale between. For example, `1..3` declares a minimum
    of 1 node and a maximum of 3. The minimum must be 0 or more, and the maximum must be 1 or at
    least match the minimum. Required.

  * `:cooldown` —  The minimum time between scaling events. Defaults to 120 seconds.

  * `:lookback` — The historic time to check queues. Defaults to 60 seconds.

  * `:queues` — Either `:all` or a list of queues to consider when measuring throughput and
    backlog.

  * `:step` — Either `:none` or the maximum nodes to scale up or down at once. Defaults to
  `:none`.

  ### Scaler Examples

  Filter throughput queries to the `:media` queue:

      scalers: [queues: :media, range: 1..3, cloud: MyApp.Cloud]

  Filter throughput queries to both `:audio` and `:video` queues:

      scalers: [queues: [:audio, :video], range: 1..3, cloud: MyApp.Cloud]

  Configure scalers driven by different queues (note, queues _may not_ overlap):

      scalers: [
        [queues: :audio, range: 0..2, cloud: {MyApp.Cloud, asg: "my-audio-asg"}],
        [queues: :video, range: 0..5, cloud: {MyApp.Cloud, asg: "my-video-asg"}]
      ]

  Limit scaling to one node up or down at a time:

      scalers: [range: 1..3, step: 1, cloud: MyApp.Cloud]

  Wait at least 5 minutes (300 seconds) between scaling events:

      scalers: [range: 1..3, cloud: MyApp.Cloud, cooldown: 300]

  Increase the period used to calculate historic throughput to 90 seconds:

      scalers: [range: 1..3, cloud: MyApp.Cloud, lookback: 90]

  > ### Scaling Down to Zero Nodes {: .warning}
  >
  > It's possible to scale down to zero nodes in staging environments or production applications
  > with periods of downtime. However, it is **only viable for multi-node setups** with dedicated
  > worker nodes and another instance type that isn't controlled by `DynamicScaler`. Without a
  > separate "web" node, or something that is always running, you run the risk of scaling down
  > without the ability to scale back up.

  ## Cloud Modules

  There are a lot of hosting platforms, aka "clouds" out there and we can't support them all! Even
  with optional dependencies, it would be a mess of libraries that may not agree with your
  application decisions. Instead, the `Oban.Pro.Cloud` behaviour defines two simple callbacks, and
  integrating with platforms typically takes a single HTTP query or library call.

  The following links contain gists of full implementations for popular cloud platforms. Feel free
  to copy-and-paste to use them as-is or as the basis for your own cloud modules.

  * [EC2](https://gist.github.com/sorentwo/ba4a07d3a011d212c19a5bb775a6c536)
  * [Fly](https://gist.github.com/sorentwo/d6be222091db7ba3c5b50d8bcabca252)
  * [GCP](https://gist.github.com/sorentwo/a54a1e2b37123fd627cca16f07aed951)
  * [Heroku](https://gist.github.com/sorentwo/54d99fb2ac05cb63ea1e30aa1935b6fc)
  * [Kubernetes](https://gist.github.com/sorentwo/f5ba9048e1e91456d37a8c7d4b8e4d58)

  Let us know if an integration for your platform is missing (which is rather likely) and you'd
  like assistance. Otherwise, follow the guide below to write your own integration!

  ### Writing Cloud Modules

  Cloud callback modules must define an `init/1` function to prepare configuration at runtime, and
  a `scale/2` callback called with the desired number of nodes and the prepared configuration.

  The following example demonstrates a complete callback module for scaling EC2 Auto Scaling
  Groups on AWS using the [SetDesiredCapacity][sdc] action. It assumes you're using the
  [ex_aws][exa] package with the proper credentials.

      defmodule MyApp.ASG do
        @behaviour Oban.Pro.Cloud

        @impl Oban.Pro.Cloud
        def init(opts), do: Map.new(opts)

        @impl Oban.Pro.Cloud
        def scale(desired, conf) do
          params = %{
            "Action" => "SetDesiredCapacity",
            "AutoScalingGroupName" => conf.asg,
            "DesiredCapacity" => desired,
            "Version" => "2011-01-01"
          }

          query = %ExAws.Operation.Query{
            path: "",
            params: params,
            service: :autoscaling,
            action: :set_desired_capacity
          }

          with {:ok, _} <- ExAws.request(query), do: {:ok, conf}
        end
      end

  You'd then use your cloud module as a scaler option:

      {DynamicScaler, scalers: [range: 1..3, cloud: {MyApp.ASG, asg: "my-asg-name"}]}

  Clouds can also pull from the application or system environment to build configuration. If your
  module pulls from the environment exclusively, then you can pass the module name rather than a
  tuple:

      {DynamicScaler, scalers: [range: 1..3, cloud: MyApp.ASG]}

  [sdc]: https://docs.aws.amazon.com/autoscaling/ec2/APIReference/API_SetDesiredCapacity.html
  [exa]: https://github.com/ex-aws/ex_aws

  ### Optimizing Throughput Queries

  While the scaler's throughput queries are optimized for a standard load, high throughput queues,
  or systems that retain a large volume of jobs, may benefit from an additional index that aids
  calculating throughput. Use the following migration to add an index if you find that scaling
  queries are too slow or timing out:

      @disable_ddl_transaction true
      @disable_migration_lock true

      def change do
        create_if_not_exists index(
          :oban_jobs,
          [:state, :queue, :attempted_at, :attempted_by],
          concurrently: true,
          where: "attempted_at IS NOT NULL",
          prefix: "public"
        )
      end

  Alternatively, you can change the timeout used for scaler inspection queries:

      {DynamicScaler, timeout: :timer.seconds(15), scalers: ...}

  ## Instrumenting with Telemetry

  The `DynamicScaler` plugin adds the following metadata to the `[:oban, :plugin, :stop]` event:

  * `:scaler` - details of the active scaler config with recent scaling values
  * `:error` — the value returned from `scale/2` when scaling fails

  When multiple `scalers` are configured one event is emitted for _each_ scaler.
  """

  @behaviour Oban.Plugin

  use GenServer

  import DateTime, only: [utc_now: 0]
  import Ecto.Query

  alias __MODULE__, as: State
  alias Oban.Plugins.Cron
  alias Oban.{Job, Peer, Repo, Validation}

  defmodule Scaler do
    @moduledoc false

    @enforce_keys [:cloud, :range]
    defstruct [
      :cloud,
      :last_rate,
      :last_scaled_at,
      :last_scaled_to,
      :last_size,
      :range,
      cooldown: 120,
      lookback: 60,
      queues: :all,
      step: :none
    ]

    def new(opts) do
      opts
      |> Keyword.update(:cloud, nil, &init_cloud/1)
      |> Keyword.update(:queues, :all, &normalize_queues/1)
      |> then(&struct!(__MODULE__, &1))
    end

    defp init_cloud(cmod) when is_atom(cmod), do: {cmod, cmod.init([])}
    defp init_cloud({cmod, copt}), do: {cmod, cmod.init(copt)}

    defp normalize_queues(:all), do: :all

    defp normalize_queues(queues) do
      queues
      |> List.wrap()
      |> Enum.map(&to_string/1)
    end
  end

  defstruct [:conf, :name, :timer, scalers: [], timeout: :timer.seconds(15)]

  defmacrop nth_elem(column, position) do
    quote do
      fragment("?[?]", unquote(column), unquote(position))
    end
  end

  @doc false
  def child_spec(args), do: super(args)

  @impl Oban.Plugin
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  @impl Oban.Plugin
  def validate(opts) do
    Validation.validate(opts, fn
      {:conf, _} -> :ok
      {:name, _} -> :ok
      {:scalers, scalers} -> validate_scalers(wrap_keyword(scalers))
      {:timeout, timeout} -> Validation.validate_timeout(:timeout, timeout)
      option -> {:unknown, option, State}
    end)
  end

  @doc false
  def optimal_scale(size, rate, min..max) do
    cond do
      rate == 0 and size > 0 and min == 0 ->
        1

      size == 0 and rate > 0 and min == 0 ->
        1

      rate == 0 ->
        min

      true ->
        optimal = size / rate

        optimal
        |> ceil()
        |> max(min)
        |> min(max)
    end
  end

  @doc false
  def clamped_scale(size, _last, _range, :none), do: size
  def clamped_scale(size, size, _range, _step), do: size

  def clamped_scale(size, last, _min..max, step) when size > last do
    (last + step)
    |> min(size)
    |> min(max)
  end

  def clamped_scale(size, last, min.._max, step) when size < last do
    (last - step)
    |> max(size)
    |> max(min)
  end

  # Callbacks

  @impl GenServer
  def init(opts) do
    Validation.validate!(opts, &validate/1)

    opts =
      Keyword.update!(opts, :scalers, fn scalers ->
        scalers
        |> wrap_keyword()
        |> Enum.map(&Scaler.new/1)
      end)

    state =
      State
      |> struct!(opts)
      |> schedule_scaling()

    :telemetry.execute([:oban, :plugin, :init], %{}, %{conf: state.conf, plugin: __MODULE__})

    {:ok, state}
  end

  defp wrap_keyword([elem | _] = list) when is_tuple(elem), do: [list]
  defp wrap_keyword(list), do: list

  @impl GenServer
  def handle_call(:check_scale, _from, %State{} = state) do
    {:reply, :ok, check_and_scale(state)}
  end

  @impl GenServer
  def handle_info(:check_scale, %State{} = state) do
    state =
      if Peer.leader?(state.conf) do
        check_and_scale(state)
      else
        state
      end

    {:noreply, schedule_scaling(state)}
  end

  # Validations

  defp validate_scalers([]), do: :ok

  defp validate_scalers([head | tail]) do
    with :ok <- Validation.validate(:scalers, head, &validate_scaler/1) do
      validate_scalers(tail)
    end
  end

  defp validate_scalers(scalers) do
    {:error, "expected :scaler to be a list of keywords, got: #{inspect(scalers)}"}
  end

  defp validate_scaler({:cloud, cloud}) do
    case cloud do
      {cmod, copt} when is_atom(cmod) and is_list(copt) ->
        :ok

      cmod when is_atom(cmod) ->
        :ok

      _ ->
        {:error,
         "expected :cloud to be a module or {module, options} tuple, got: #{inspect(cloud)}"}
    end
  end

  defp validate_scaler({:cooldown, cooldown}) do
    Validation.validate_integer(:cooldown, cooldown, min: 0)
  end

  defp validate_scaler({:lookback, cooldown}) do
    Validation.validate_integer(:lookback, cooldown)
  end

  defp validate_scaler({:queues, queues}) do
    case queues do
      :all ->
        :ok

      queue when is_atom(queue) and not is_nil(queue) ->
        :ok

      [queue | _] when is_atom(queue) or is_binary(queue) ->
        :ok

      _ ->
        {:error, "expected :queues to be :all or a list of queue names, got: #{inspect(queues)}"}
    end
  end

  defp validate_scaler({:range, range}) do
    case range do
      min..max when min <= max and max > 0 ->
        :ok

      _min.._max ->
        {:error, "expected :range to have a min less than max, got: #{inspect(range)}"}

      _ ->
        {:error, "exepcted :range to be a range of integers, got: #{inspect(range)}"}
    end
  end

  defp validate_scaler({:step, :none}), do: :ok

  defp validate_scaler({:step, limit}) do
    Validation.validate_integer(:step, limit)
  end

  # Necessary to seed testing, not documented
  defp validate_scaler({:last_rate, _}), do: :ok
  defp validate_scaler({:last_scaled_at, _}), do: :ok
  defp validate_scaler({:last_scaled_to, _}), do: :ok
  defp validate_scaler({:last_size, _}), do: :ok

  defp validate_scaler(option), do: {:unknown, option, Scaler}

  # Scheduling

  defp schedule_scaling(state) do
    timer = Process.send_after(self(), :check_scale, Cron.interval_to_next_minute())

    %{state | timer: timer}
  end

  # Scaling

  defp check_and_scale(%State{scalers: scalers} = state) do
    scalers =
      for scaler <- scalers do
        meta = %{conf: state.conf, plugin: __MODULE__, scaler: scaler}

        :telemetry.span([:oban, :plugin], meta, fn ->
          since = DateTime.add(utc_now(), -scaler.lookback, :second)
          last_size = scaler.last_size

          scaler =
            scaler
            |> record_size(since, state)
            |> record_rate(since, state)

          case apply_scale(scaler, last_size) do
            {:ok, scaler} ->
              {scaler, Map.put(meta, :scaler, scaler)}

            {:error, reason} ->
              {scaler, Map.put(meta, :error, reason)}
          end
        end)
      end

    %{state | scalers: scalers}
  end

  defp record_size(scaler, since, state) do
    query =
      scaler.queues
      |> base_query()
      |> where([j], j.state in ~w(available scheduled))
      |> where([j], j.scheduled_at > ^since)
      |> select(count())

    %{scaler | last_size: Repo.one(state.conf, query, timeout: state.timeout)}
  end

  defp record_rate(scaler, since, state) do
    query =
      scaler.queues
      |> base_query()
      |> where([j], j.state in ~w(executing retryable completed cancelled discarded))
      |> where([j], j.attempted_at > ^since)
      |> group_by([j], nth_elem(j.attempted_by, 1))
      |> select([j], {nth_elem(j.attempted_by, 1), count()})

    case Repo.all(state.conf, query, timeout: state.timeout) do
      [] ->
        %{scaler | last_rate: 0, last_scaled_to: scaler.last_scaled_to || 0}

      node_counts ->
        size = length(node_counts)

        rate =
          node_counts
          |> Enum.map(&elem(&1, 1))
          |> Enum.sum()
          |> div(size)

        %{scaler | last_rate: rate, last_scaled_to: scaler.last_scaled_to || size}
    end
  end

  defp base_query(queues) do
    if queues == :all do
      where(Job, [j], not is_nil(j.queue))
    else
      where(Job, [j], j.queue in ^queues)
    end
  end

  defp apply_scale(scaler, prev_size) do
    scale_to =
      prev_size
      |> next_size(scaler.last_size)
      |> optimal_scale(scaler.last_rate, scaler.range)
      |> clamped_scale(scaler.last_scaled_to, scaler.range, scaler.step)

    cond do
      recently_scaled?(scaler) ->
        {:ok, scaler}

      already_scaled?(scale_to, scaler) ->
        {:ok, scaler}

      true ->
        {cloud_mod, cloud_opt} = scaler.cloud

        case cloud_mod.scale(scale_to, cloud_opt) do
          {:ok, cloud_opt} ->
            cloud = {cloud_mod, cloud_opt}

            {:ok, %{scaler | cloud: cloud, last_scaled_at: utc_now(), last_scaled_to: scale_to}}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  defp next_size(nil, size), do: size
  defp next_size(prev, curr), do: max(curr + (curr - prev), 0)

  defp recently_scaled?(%{last_scaled_at: nil}), do: false

  defp recently_scaled?(%{cooldown: cooldown, last_scaled_at: scaled_at}) do
    scaling_allowed_at = DateTime.add(scaled_at, cooldown)

    DateTime.compare(utc_now(), scaling_allowed_at) != :lt
  end

  defp already_scaled?(scale_to, %{last_scaled_to: to}), do: scale_to == to
end

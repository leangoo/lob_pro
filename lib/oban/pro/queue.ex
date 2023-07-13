defmodule Oban.Pro.Queue do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  alias Ecto.Changeset
  alias Oban.Pro.{Producer, Utils}

  @primary_key {:name, :string, autogenerate: false}
  schema "oban_queues" do
    field :lock_version, :integer, default: 1

    embeds_one :only, Only, on_replace: :update, primary_key: false do
      @moduledoc false

      field :mode, Ecto.Enum, values: [:node, :sys_env]
      field :op, Ecto.Enum, values: [:==, :!=, :=~]
      field :key, :string
      field :value, :string
    end

    embeds_one :opts, Opts, on_replace: :update, primary_key: false do
      @moduledoc false

      field :local_limit, :integer
      field :paused, :boolean
      field :retry_attempts, :integer, default: 5
      field :retry_backoff, :integer, default: :timer.seconds(1)

      embeds_one :global_limit, GlobalLimit, on_replace: :update, primary_key: false do
        @moduledoc false

        field :allowed, :integer

        embeds_one :partition, Partition, on_replace: :update, primary_key: false do
          @moduledoc false

          field :fields, {:array, :string}
          field :keys, {:array, :string}
        end
      end

      embeds_one :rate_limit, RateLimit, on_replace: :update, primary_key: false do
        @moduledoc false

        field :allowed, :integer
        field :period, :integer

        embeds_one :partition, Partition, on_replace: :update, primary_key: false do
          @moduledoc false

          field :fields, {:array, Ecto.Enum}, values: [:args, :worker]
          field :keys, {:array, :string}
        end
      end
    end

    timestamps(
      inserted_at: :inserted_at,
      updated_at: :updated_at,
      type: :utc_datetime_usec
    )
  end

  @spec changeset(Keyword.t() | {atom() | binary(), pos_integer() | Keyword.t()}) :: Changeset.t()
  def changeset([_ | _] = params) do
    changeset(%__MODULE__{}, Map.new(params))
  end

  def changeset({name, opts}) when is_list(opts) do
    changeset(%__MODULE__{}, %{name: to_string(name), opts: Map.new(opts)})
  end

  def changeset({name, limit}) do
    changeset(%__MODULE__{}, %{name: to_string(name), opts: %{local_limit: limit}})
  end

  @doc false
  @spec changeset(Ecto.Schema.t(), map()) :: Changeset.t()
  def changeset(schema, %{opts: %{only: _}} = params) do
    {only, params} = pop_in(params, [:opts, :only])

    params = Map.put(params, :only, cast_only(only))

    changeset(schema, params)
  end

  def changeset(schema, params) do
    schema
    |> cast(params, [:name])
    |> cast_embed(:only, with: &only_changeset/2)
    |> cast_embed(:opts, required: true, with: &opts_changeset/2)
    |> validate_required([:name])
    |> validate_length(:name, min: 1)
    |> optimistic_lock(:lock_version)
    |> Utils.enforce_keys(params, __MODULE__)
  end

  defp only_changeset(schema, params) do
    schema
    |> cast(params, ~w(mode op key value)a)
    |> validate_required(~w(mode op value)a)
    |> Utils.enforce_keys(params, __MODULE__.Only)
  end

  defp opts_changeset(schema, params) do
    params =
      params
      |> Map.new(fn {key, val} -> {Utils.maybe_to_atom(key), val} end)
      |> Map.delete(:limit)
      |> Map.put_new_lazy(:local_limit, fn -> Producer.default_local_limit(params, schema) end)

    # NOTE: Switch to `replace_lazy/3` when we require Elixir 1.14+
    params =
      if Map.get(params, :global_limit) do
        Map.update!(params, :global_limit, &Producer.cast_global_limit/1)
      else
        params
      end

    params =
      if Map.get(params, :rate_limit) do
        Map.update!(params, :rate_limit, &Producer.cast_rate_limit/1)
      else
        params
      end

    schema
    |> cast(params, ~w(local_limit paused retry_attempts retry_backoff)a)
    |> cast_embed(:global_limit, with: &Producer.global_changeset/2)
    |> cast_embed(:rate_limit, with: &rate_changeset/2)
    |> validate_required(~w(local_limit)a)
    |> validate_number(:local_limit, greater_than: 0)
    |> validate_number(:retry_attempts, greater_than: 0)
    |> validate_number(:retry_backoff, greater_than: 0)
    |> Producer.validate_single_partitioner()
    |> Utils.enforce_keys(params, __MODULE__.Opts)
  end

  defp rate_changeset(schema, params) do
    params = Map.update(params, :period, nil, &Producer.cast_period/1)

    schema
    |> cast(params, ~w(allowed period)a)
    |> cast_embed(:partition, with: &partition_changeset/2)
    |> validate_required(~w(allowed period)a)
    |> validate_number(:allowed, greater_than: 0)
    |> validate_number(:period, greater_than: 0)
    |> Utils.enforce_keys(params, __MODULE__.Opts.RateLimit)
  end

  defp partition_changeset(schema, params) do
    params = Map.update(params, :keys, [], &Utils.maybe_stringify_list/1)

    schema
    |> cast(params, ~w(fields keys)a)
    |> validate_required(~w(fields)a)
    |> Utils.enforce_keys(params, __MODULE__.Opts.RateLimit.Partition)
  end

  @spec to_keyword_opts(%{opts: map()} | map()) :: Keyword.t()
  def to_keyword_opts(%__MODULE__{name: queue, opts: opts}) do
    opts
    |> Ecto.embedded_dump(:json)
    |> Map.put(:queue, queue)
    |> to_keyword_opts()
  end

  def to_keyword_opts(opts) do
    for {key, val} <- opts, not is_nil(val), do: {Utils.maybe_to_atom(key), val}
  end

  # Helpers

  defp cast_only({:node, value}), do: cast_only({:node, :==, value})
  defp cast_only({:node, op, value}), do: %{mode: :node, op: op, value: to_string(value)}

  defp cast_only({:sys_env, key, value}), do: cast_only({:sys_env, key, :==, value})

  defp cast_only({:sys_env, key, op, value}) do
    %{mode: :sys_env, op: op, key: key, value: to_string(value)}
  end

  defp cast_only(other), do: other
end

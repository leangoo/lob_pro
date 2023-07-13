require Protocol

defmodule Oban.Pro.Producer do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  alias Ecto.Changeset
  alias Oban.Pro.Utils

  @type t :: %__MODULE__{
          uuid: Ecto.UUID.t(),
          name: binary(),
          node: binary(),
          queue: binary(),
          meta: map(),
          started_at: DateTime.t(),
          updated_at: DateTime.t()
        }

  @primary_key {:uuid, :binary_id, autogenerate: true}
  schema "oban_producers" do
    field :name, :string
    field :node, :string
    field :queue, :string
    field :refresh_interval, :integer, virtual: true, default: :timer.seconds(30)
    field :started_at, :utc_datetime_usec
    field :updated_at, :utc_datetime_usec

    embeds_one :meta, Meta, on_replace: :update, primary_key: false do
      @moduledoc false

      field :local_limit, :integer, default: 10
      field :paused, :boolean, default: false
      field :retry_attempts, :integer, default: 5
      field :retry_backoff, :integer, default: :timer.seconds(1)

      embeds_one :global_limit, GlobalLimit, on_replace: :update, primary_key: false do
        @moduledoc false

        field :allowed, :integer
        field :tracked, :map, default: %{}

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
        field :window_time, :integer, skip_default_validation: true
        field :windows, {:array, :map}, default: []

        embeds_one :partition, Partition, on_replace: :update, primary_key: false do
          @moduledoc false

          field :fields, {:array, :string}
          field :keys, {:array, :string}
        end
      end
    end
  end

  @spec new(map() | Keyword.t()) :: Changeset.t(t())
  def new(params) when is_list(params) or is_map(params) do
    params =
      params
      |> Map.new()
      |> Map.update!(:name, &to_clean_string/1)
      |> Map.update!(:node, &to_clean_string/1)
      |> Map.update!(:queue, &to_clean_string/1)

    %__MODULE__{}
    |> cast(params, ~w(name node queue refresh_interval started_at updated_at)a)
    |> cast_embed(:meta, required: true, with: &meta_changeset/2)
    |> validate_required(~w(name node queue)a)
    |> validate_length(:name, min: 1)
    |> validate_length(:node, min: 1)
    |> validate_length(:queue, min: 1)
    |> Utils.enforce_keys(params, __MODULE__)
  end

  @spec update_meta(t(), atom(), term()) :: Changeset.t()
  def update_meta(schema, key, value) do
    meta =
      schema.meta
      |> meta_changeset(%{key => value})
      |> apply_changes()
      |> Ecto.embedded_dump(:json)

    schema
    |> cast(%{meta: meta}, [])
    |> cast_embed(:meta, with: &meta_changeset/2)
  end

  @doc false
  @spec meta_changeset(Ecto.Schema.t(), map() | Keyword.t()) :: Changeset.t()
  def meta_changeset(schema, params) do
    params =
      params
      |> Map.new()
      |> Map.delete(:limit)
      |> Map.put_new_lazy(:local_limit, fn -> default_local_limit(params, schema) end)

    # NOTE: Switch to `replace_lazy/3` when we require Elixir 1.14+
    params =
      if Map.get(params, :global_limit) do
        Map.update!(params, :global_limit, &cast_global_limit/1)
      else
        params
      end

    params =
      if Map.get(params, :rate_limit) do
        Map.update!(params, :rate_limit, &cast_rate_limit/1)
      else
        params
      end

    schema
    |> cast(params, ~w(local_limit paused retry_attempts retry_backoff)a)
    |> cast_embed(:global_limit, with: &global_changeset/2)
    |> cast_embed(:rate_limit, with: &rate_changeset/2)
    |> validate_required(~w(local_limit)a)
    |> validate_number(:local_limit, greater_than: 0)
    |> validate_number(:retry_attempts, greater_than: 0)
    |> validate_number(:retry_backoff, greater_than: 0)
    |> validate_single_partitioner()
    |> Utils.enforce_keys(params, __MODULE__.Meta)
  end

  @doc false
  @spec default_local_limit(map(), Ecto.Schema.t()) :: non_neg_integer()
  def default_local_limit(params, schema) do
    cond do
      is_integer(params[:limit]) ->
        params[:limit]

      is_integer(params[:global_limit]) ->
        params[:global_limit]

      is_integer(get_in(params, [:global_limit, :allowed])) ->
        get_in(params, [:global_limit, :allowed])

      true ->
        schema.local_limit
    end
  end

  @doc false
  @spec validate_single_partitioner(Changeset.t()) :: Changeset.t()
  def validate_single_partitioner(changeset) do
    case {get_field(changeset, :global_limit), get_field(changeset, :rate_limit)} do
      {%{partition: %{}}, %{partition: %{}}} ->
        add_error(changeset, :global_limit, "only one limiter may have partitioning")

      _ ->
        changeset
    end
  end

  @doc false
  @spec global_changeset(Ecto.Schema.t(), integer() | map() | Keyword.t()) :: Changeset.t()
  def global_changeset(schema, params) do
    schema
    |> cast(params, ~w(allowed)a)
    |> cast_embed(:partition, with: &partition_changeset/2)
    |> validate_number(:allowed, greater_than: 0)
    |> Utils.enforce_keys(params, __MODULE__.Meta.GlobalLimit)
  end

  @doc false
  @spec rate_changeset(Ecto.Schema.t(), map() | Keyword.t()) :: Changeset.t()
  def rate_changeset(schema, params) do
    params =
      params
      |> Map.update(:period, nil, &cast_period/1)
      |> Map.put_new_lazy(:window_time, fn -> DateTime.to_unix(DateTime.utc_now(), :second) end)

    schema
    |> cast(params, ~w(allowed period window_time)a)
    |> cast_embed(:partition, with: &partition_changeset/2)
    |> validate_required(~w(allowed period)a)
    |> validate_number(:allowed, greater_than: 0)
    |> validate_number(:period, greater_than: 0)
    |> Utils.enforce_keys(params, __MODULE__.Meta.RateLimit)
  end

  @doc false
  @spec partition_changeset(Ecto.Schema.t(), map() | Keyword.t()) :: Changeset.t()
  def partition_changeset(schema, params) do
    params =
      params
      |> Map.new()
      |> Map.update(:fields, [], &Utils.maybe_stringify_list/1)
      |> Map.update(:keys, [], &Utils.maybe_stringify_list/1)

    schema
    |> cast(params, ~w(fields keys)a)
    |> validate_required(~w(fields)a)
    |> validate_subset(:fields, ~w(worker args))
    |> Utils.enforce_keys(params, __MODULE__.Meta.RateLimit.Partition)
  end

  # Global Limit Helpers

  @doc false
  @spec cast_global_limit(pos_integer() | list() | map()) :: map()
  def cast_global_limit(limit) when is_integer(limit) do
    %{allowed: limit}
  end

  def cast_global_limit([[key | _] | _] = opts) when is_binary(key) do
    opts
    |> Enum.map(&List.to_tuple/1)
    |> Map.new()
    |> cast_global_limit()
  end

  def cast_global_limit(opts), do: opts

  # Rate Limit Helpers

  @doc false
  @spec cast_rate_limit(list() | map()) :: map()
  def cast_rate_limit([[key | _] | _] = opts) when is_binary(key) do
    opts
    |> Enum.map(&List.to_tuple/1)
    |> Map.new()
    |> cast_rate_limit()
  end

  def cast_rate_limit(%{"allowed" => _} = opts) do
    Map.new(opts, fn
      {"allowed", allowed} ->
        {:allowed, allowed}

      {"period", [time, unit]} ->
        {:period, {time, unit}}

      {"period", period} ->
        {:period, period}

      {"partition", [["fields", fields]]} ->
        {:partition, fields: fields}

      {"partition", [["fields", fields], ["keys", keys]]} ->
        {:partition, fields: fields, keys: keys}
    end)
  end

  def cast_rate_limit(opts), do: opts

  @spec cast_period(pos_integer() | {atom(), pos_integer()}) :: pos_integer()
  def cast_period({value, unit}) do
    unit = to_string(unit)

    cond do
      unit in ~w(second seconds) -> value
      unit in ~w(minute minutes) -> value * 60
      unit in ~w(hour hours) -> value * 60 * 60
      unit in ~w(day days) -> value * 24 * 60 * 60
      true -> unit
    end
  end

  def cast_period(period), do: period

  # Helpers

  defp to_clean_string(value) when is_reference(value) do
    inspect(value)
  end

  defp to_clean_string(value) do
    value
    |> to_string()
    |> String.trim_leading("Elixir.")
  end
end

Protocol.derive(Jason.Encoder, Oban.Pro.Producer, except: [:__meta__])
Protocol.derive(Jason.Encoder, Oban.Pro.Producer.Meta.GlobalLimit)
Protocol.derive(Jason.Encoder, Oban.Pro.Producer.Meta.GlobalLimit.Partition)
Protocol.derive(Jason.Encoder, Oban.Pro.Producer.Meta.RateLimit)
Protocol.derive(Jason.Encoder, Oban.Pro.Producer.Meta.RateLimit.Partition)

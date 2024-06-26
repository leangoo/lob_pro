defmodule Oban.Pro.Cron do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  alias Oban.Worker
  alias Oban.Cron.Expression

  @primary_key {:name, :string, autogenerate: false}
  schema "oban_crons" do
    field :expression, :string
    field :worker, :string
    field :opts, :map
    field :paused, :boolean
    field :lock_version, :integer, default: 1

    timestamps(
      inserted_at: :inserted_at,
      updated_at: :updated_at,
      type: :utc_datetime_usec
    )
  end

  @permitted ~w(name expression worker opts paused)a
  @requried ~w(name expression worker)a
  @allowed_opts ~w(args max_attempts meta priority queue tags timezone)a

  @doc false
  def allowed_opts, do: @allowed_opts

  @spec changeset({binary(), module()} | {binary(), module(), Keyword.t()}) :: Ecto.Changeset.t()
  def changeset({expression, worker}) do
    params = %{expression: expression, name: worker, worker: worker, opts: %{}}

    changeset(%__MODULE__{}, params)
  end

  def changeset({expression, worker, opts}) do
    {name, opts} = Keyword.pop(opts, :name, worker)
    {paused, opts} = Keyword.pop(opts, :paused)

    params = %{
      expression: expression,
      worker: worker,
      name: name,
      opts: Map.new(opts),
      paused: paused
    }

    changeset(%__MODULE__{}, params)
  end

  @spec changeset({Ecto.Schema.t(), map()}) :: Ecto.Changeset.t()
  def changeset(schema, params) when is_map(params) do
    params =
      params
      |> coerce_name(:name)
      |> coerce_name(:worker)
      |> merge_opts(schema)

    schema
    |> cast(params, @permitted)
    |> validate_required(@requried)
    |> validate_change(:expression, &expression_validator/2)
    |> validate_change(:opts, &opts_validator/2)
    |> optimistic_lock(:lock_version)
  end

  defp coerce_name(params, key) do
    case params do
      %{^key => value} when is_atom(value) ->
        Map.put(params, key, Worker.to_string(value))

      _ ->
        params
    end
  end

  defp merge_opts(params, schema) do
    case Map.split(params, @allowed_opts) do
      {opts, params} when map_size(opts) > 0 ->
        opts = Map.new(opts, fn {key, val} -> {to_string(key), val} end)

        Map.put(params, :opts, Map.merge(schema.opts, opts))

      _ ->
        params
    end
  end

  # Validators

  defp expression_validator(:expression, expression) do
    Expression.parse!(expression)

    []
  rescue
    ArgumentError ->
      [expression: "expected cron expression to be a parsable binary"]
  end

  defp opts_validator(:opts, opts) do
    string_keys = Enum.map(@allowed_opts, &to_string/1)

    if Enum.all?(opts, fn {key, _} -> to_string(key) in string_keys end) do
      []
    else
      [opts: "expected cron opts to be one of #{inspect(@allowed_opts)}"]
    end
  end
end

defmodule Oban.Pro.Stages.Structured do
  @moduledoc false

  @behaviour Oban.Pro.Stage

  alias Ecto.Changeset
  alias Oban.Pro.Utils

  defstruct [:worker]

  @impl Oban.Pro.Stage
  def init(worker, _opts) do
    if function_exported?(worker, :__args_schema__, 0) do
      {:ok, %__MODULE__{worker: worker}}
    else
      {:ok, :ignore}
    end
  end

  @impl Oban.Pro.Stage
  def before_new(args, opts, :ignore), do: {:ok, args, opts}

  def before_new(args, opts, conf) do
    {:ok, _changes} = gather_changes(conf.worker, args)

    meta = %{"structured" => true}
    opts = Keyword.update(opts, :meta, meta, &Map.merge(&1, meta))

    {:ok, args, opts}
  catch
    message -> {:error, message}
  end

  @impl Oban.Pro.Stage
  def before_process(job, :ignore), do: {:ok, job}

  def before_process(%{args: args} = job, conf) do
    {:ok, changes} = gather_changes(conf.worker, args)

    {:ok, %{job | args: changes}}
  catch
    message -> {:error, message}
  end

  defp gather_changes(module, args) do
    fields = module.__args_schema__()
    struct = struct(module, [])

    {fields, required, merge} = split_fields(fields, args)

    keys = Map.keys(fields)

    changeset =
      {struct, fields}
      |> Changeset.cast(args, keys)
      |> Changeset.validate_required(required)
      |> validate_allowed(args, keys)

    if changeset.valid? do
      changeset
      |> Changeset.apply_changes()
      |> Map.merge(merge)
      |> then(&{:ok, &1})
    else
      changeset
      |> Utils.to_translated_errors()
      |> Enum.map_join(", ", fn {field, error} -> "#{inspect(field)} #{error}" end)
      |> throw()
    end
  end

  defp split_fields(fields, args) do
    Enum.reduce(fields, {%{}, [], %{}}, fn {name, opts}, {keep, required, merge} ->
      required = if opts[:required], do: [name | required], else: required

      case Map.new(opts) do
        %{cardinality: :one, module: module, type: :embed} ->
          {:ok, embed} = gather_changes(module, args[name] || args[to_string(name)] || %{})

          {Map.put(keep, name, :any), required, Map.put(merge, name, embed)}

        %{cardinality: :many, module: module, type: :embed} ->
          embed_list =
            for sub_arg <- args[name] || args[to_string(name)] || [] do
              {:ok, embed} = gather_changes(module, sub_arg)

              embed
            end

          {Map.put(keep, name, :any), required, Map.put(merge, name, embed_list)}

        %{type: :enum} ->
          enum = {:parameterized, Ecto.Enum, Ecto.Enum.init(values: opts[:values])}

          {Map.put(keep, name, enum), required, merge}

        %{type: :uuid} ->
          {Map.put(keep, name, :binary_id), required, merge}

        %{type: {:array, :uuid}} ->
          {Map.put(keep, name, {:array, :binary_id}), required, merge}

        %{type: type} ->
          {Map.put(keep, name, type), required, merge}
      end
    end)
  end

  defp validate_allowed(changeset, args, keys) do
    keys = MapSet.new(keys, &to_string/1)

    Enum.reduce(args, changeset, fn {key, _val}, changeset ->
      if to_string(key) in keys do
        changeset
      else
        Changeset.add_error(changeset, key, "is an unexpected key")
      end
    end)
  end
end

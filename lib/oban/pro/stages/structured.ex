defmodule Oban.Pro.Stages.Structured do
  @moduledoc false

  @behaviour Oban.Pro.Stage

  defstruct [:worker, keys: [], required: []]

  @impl Oban.Pro.Stage
  def init(worker, opts) do
    with {:ok, keys} <- fetch_opt(opts, :keys),
         {:ok, required} <- fetch_opt(opts, :required),
         :ok <- check_subset(keys, required) do
      {:ok, %__MODULE__{keys: keys, required: required, worker: worker}}
    end
  end

  @impl Oban.Pro.Stage
  def before_new(args, opts, conf) do
    with :ok <- check_args(args, conf) do
      meta = %{"structured" => true}
      opts = Keyword.update(opts, :meta, meta, &Map.merge(&1, meta))

      {:ok, args, opts}
    end
  end

  @impl Oban.Pro.Stage
  def before_process(%{args: args} = job, conf) do
    with :ok <- check_args(args, conf) do
      args = Map.new(args, fn {key, val} -> {String.to_existing_atom(key), val} end)

      {:ok, %{job | args: struct!(conf.worker, args)}}
    end
  end

  # Helpers

  defp fetch_opt(opts, key) do
    set =
      opts
      |> Keyword.get(key, [])
      |> MapSet.new(&to_string/1)

    {:ok, set}
  end

  defp check_subset(keys, required) do
    if MapSet.subset?(required, keys) do
      :ok
    else
      error("some :required aren't included in :keys ", required, keys)
    end
  end

  defp check_args(args, %{keys: keys, required: required}) do
    args_keys =
      args
      |> Map.keys()
      |> MapSet.new(&to_string/1)

    cond do
      not MapSet.subset?(args_keys, keys) ->
        error("unexpected keys: ", args_keys, keys)

      MapSet.size(required) > 0 and not MapSet.subset?(required, args_keys) ->
        error("missing required keys: ", required, args_keys)

      true ->
        :ok
    end
  end

  defp error(message, set_a, set_b) do
    diff =
      set_a
      |> MapSet.difference(set_b)
      |> MapSet.to_list()

    {:error, message <> inspect(diff)}
  end
end

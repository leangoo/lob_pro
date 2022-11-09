defmodule Oban.Pro.Stages.Encrypted do
  @moduledoc false

  @behaviour Oban.Pro.Stage

  defstruct [:key, cipher: :aes_256_ctr, iv_bytes: 16]

  @impl Oban.Pro.Stage
  def init(_worker \\ nil, opts) do
    case Keyword.fetch(opts, :key) do
      {:ok, key} when is_binary(key) ->
        {:ok, %__MODULE__{key: Base.decode64!(key)}}

      {:ok, {_mod, _fun, _arg} = key} ->
        {:ok, %__MODULE__{key: key}}

      _ ->
        {:error, "expected :key to be a binary or mfa, got: #{inspect(opts)}"}
    end
  end

  @impl Oban.Pro.Stage
  def before_new(args, opts, conf) do
    iv = :crypto.strong_rand_bytes(conf.iv_bytes)

    meta = %{"encrypted" => true, "iv" => Base.encode64(iv)}

    opts =
      opts
      |> Keyword.put_new(:meta, %{})
      |> Keyword.update!(:meta, &Map.merge(&1, meta))

    {:ok, %{"data" => encode(args, iv, conf)}, opts}
  end

  @impl Oban.Pro.Stage
  def before_process(%{args: args, meta: meta} = job, conf) do
    with %{"encrypted" => true, "iv" => iv} <- meta,
         %{"data" => data} <- args do
      {:ok, %{job | args: decode(data, iv, conf)}}
    else
      _ ->
        {:ok, job}
    end
  end

  # Helpers

  defp resolve_key({mod, fun, arg}) do
    mod
    |> apply(fun, arg)
    |> Base.decode64!()
  end

  defp resolve_key(key), do: key

  defp encode(data, iv, conf) do
    key = resolve_key(conf.key)
    json = Jason.encode!(data)

    conf.cipher
    |> :crypto.crypto_one_time(key, iv, json, true)
    |> Base.encode64()
  rescue
    error -> reraise error, prune_args_from_stacktrace(__STACKTRACE__)
  end

  defp decode(data, iv, conf) do
    key = resolve_key(conf.key)

    conf.cipher
    |> :crypto.crypto_one_time(key, Base.decode64!(iv), Base.decode64!(data), false)
    |> Jason.decode!()
  rescue
    error -> reraise error, prune_args_from_stacktrace(__STACKTRACE__)
  end

  defp prune_args_from_stacktrace([{mod, fun, [_ | _] = args, info} | rest]) do
    [{mod, fun, length(args), info} | rest]
  end

  defp prune_args_from_stacktrace(stacktrace), do: stacktrace
end

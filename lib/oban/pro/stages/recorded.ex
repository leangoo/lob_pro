defmodule Oban.Pro.Stages.Recorded do
  @moduledoc false

  @behaviour Oban.Pro.Stage

  alias Oban.Pro.Utils

  defstruct to: :return, limit: 32_000, safe_decode: false

  @impl Oban.Pro.Stage
  def init(_worker \\ nil, opts)

  def init(_worker, true) do
    {:ok, %__MODULE__{}}
  end

  def init(_worker, opts) do
    with :ok <- Utils.validate_opts(opts, &validate_opt/1) do
      {:ok, struct(__MODULE__, opts)}
    end
  end

  @impl Oban.Pro.Stage
  def before_new(args, opts, _conf) do
    meta = %{"recorded" => true}
    opts = Keyword.update(opts, :meta, meta, &Map.merge(&1, meta))

    {:ok, args, opts}
  end

  @impl Oban.Pro.Stage
  def after_process(result, job, conf) do
    with {:ok, return} <- result do
      encoded = Utils.encode64(return)

      if byte_size(encoded) <= conf.limit do
        send(self(), {:record_meta, Map.put(job.meta, to_string(conf.to), encoded)})

        :ok
      else
        {:error, "return is #{byte_size(encoded)} bytes, larger than the limit: #{conf.limit}"}
      end
    end
  end

  @spec fetch_recorded(Oban.Job.t(), conf :: map()) :: {:ok, term()} | {:error, term()}
  def fetch_recorded(job, conf) do
    to = to_string(conf.to)

    case job.meta do
      %{"recorded" => true, ^to => value} ->
        {:ok, Utils.decode64(value, decode_opts(job.meta))}

      _ ->
        {:error, :missing}
    end
  end

  # Helpers

  defp validate_opt({:to, to}) do
    unless (not is_nil(to) and is_atom(to)) or is_binary(to) do
      {:error, "expected :to to be an atom or binary, got: #{inspect(to)}"}
    end
  end

  defp validate_opt({:limit, limit}) do
    unless is_integer(limit) and limit > 0 do
      {:error, "expected :limit to be a positive integer, got: #{inspect(limit)}"}
    end
  end

  defp validate_opt({:safe_decode, safe}) do
    unless is_boolean(safe) do
      {:error, "expected :safe_decode to be a boolean, got: #{inspect(safe)}"}
    end
  end

  defp decode_opts(%{"safe_decode" => true}), do: [:safe]
  defp decode_opts(_meta), do: []
end

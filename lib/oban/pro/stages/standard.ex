defmodule Oban.Pro.Stages.Standard do
  @moduledoc false

  @behaviour Oban.Pro.Stage

  alias Oban.Job
  alias Oban.Pro.Utils

  @impl Oban.Pro.Stage
  def init(_worker \\ nil, opts) do
    with :ok <- Utils.validate_opts(opts, &validate_opt/1) do
      {:ok, []}
    end
  end

  # Helpers

  defp validate_opt({:max_attempts, max_attempts}) do
    unless is_integer(max_attempts) and max_attempts > 0 do
      {:error, "expected :max_attempts to be a positive integer, got: #{inspect(max_attempts)}"}
    end
  end

  defp validate_opt({:priority, priority}) do
    unless is_integer(priority) and priority > -1 and priority < 4 do
      {:error, "expected :priority to be an integer from 0 to 3, got: #{inspect(priority)}"}
    end
  end

  defp validate_opt({:queue, queue}) do
    unless is_atom(queue) or is_binary(queue) do
      {:error, "expected :queue to be an atom or a binary, got: #{inspect(queue)}"}
    end
  end

  defp validate_opt({:tags, tags}) do
    unless is_list(tags) and Enum.all?(tags, &is_binary/1) do
      {:error, "expected :tags to be a list of strings, got: #{inspect(tags)}"}
    end
  end

  defp validate_opt({:unique, unique}) do
    unless is_list(unique) and Enum.all?(unique, &Job.valid_unique_opt?/1) do
      {:error, "unexpected unique options: #{inspect(unique)}"}
    end
  end

  defp validate_opt(option) do
    {:error, "unknown option provided #{inspect(option)}"}
  end
end

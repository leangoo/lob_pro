defmodule Oban.Pro.Validation do
  @moduledoc false

  # Shared validations for use with `Oban.Validation.validate/2,3`

  @doc """
  Validate `by:` options for partitioning workers.
  """
  def validate_by(:worker), do: :ok
  def validate_by([{:args, key}]) when is_atom(key), do: :ok
  def validate_by([{:meta, key}]) when is_atom(key), do: :ok
  def validate_by([{:args, [key | _]}]) when is_atom(key), do: :ok
  def validate_by([{:meta, [key | _]}]) when is_atom(key), do: :ok
  def validate_by([:worker, {:args, key}]) when is_atom(key), do: :ok
  def validate_by([:worker, {:meta, key}]) when is_atom(key), do: :ok
  def validate_by([:worker, {:args, [key | _]}]) when is_atom(key), do: :ok
  def validate_by([:worker, {:meta, [key | _]}]) when is_atom(key), do: :ok

  def validate_by(fields) do
    {:error, "expected :by to be :worker or an :args/:meta tuple, got: #{inspect(fields)}"}
  end
end

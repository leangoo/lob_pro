defmodule Oban.Pro.Utils do
  @moduledoc false

  alias Ecto.Changeset

  @spec encode64(term(), [:compressed | :deterministic]) :: String.t()
  def encode64(term, opts \\ [:compressed]) do
    term
    |> :erlang.term_to_binary(opts)
    |> Base.encode64(padding: false)
  end

  @spec decode64(binary(), [:safe | :used]) :: term()
  def decode64(bin, opts \\ [:safe]) do
    bin
    |> Base.decode64!(padding: false)
    |> :erlang.binary_to_term(opts)
  end

  @spec validate_opts(list(), fun()) :: :ok | {:error, term()}
  def validate_opts(opts, validator) do
    Enum.reduce_while(opts, :ok, fn opt, acc ->
      case validator.(opt) do
        {:error, _reason} = error -> {:halt, error}
        _ -> {:cont, acc}
      end
    end)
  end

  @spec enforce_keys(Changeset.t(), map(), module()) :: Changeset.t()
  def enforce_keys(changeset, params, schema) do
    fields =
      [:fields, :virtual_fields]
      |> Enum.flat_map(&schema.__schema__/1)
      |> Enum.map(&to_string/1)

    Enum.reduce(params, changeset, fn {key, _val}, acc ->
      if to_string(key) in fields do
        acc
      else
        Changeset.add_error(acc, :base, "unknown field #{key} provided")
      end
    end)
  end

  @spec to_uniq_key(Changeset.t()) :: nil | non_neg_integer()
  def to_uniq_key(%{changes: %{unique: %{fields: fields, keys: keys}}} = changeset) do
    fields
    |> Enum.map(fn
      :args -> take_keys(changeset, :args, keys)
      :meta -> take_keys(changeset, :meta, keys)
      field -> Changeset.get_field(changeset, field)
    end)
    |> :erlang.phash2()
  end

  def to_uniq_key(_changeset), do: nil

  defp take_keys(changeset, field, keys) do
    normalized =
      changeset
      |> Changeset.get_field(field)
      |> Map.new(fn {key, val} -> {to_string(key), val} end)

    if keys == [] do
      normalized
    else
      Map.take(normalized, Enum.map(keys, &to_string/1))
    end
  end

  def normalize_by(by) do
    by
    |> List.wrap()
    |> Enum.map(fn
      {key, val} -> [key, List.wrap(val)]
      field -> field
    end)
  end

  @spec to_exception(Changeset.t()) :: Exception.t()
  def to_exception(changeset) do
    changeset
    |> to_translated_errors()
    |> Enum.reverse()
    |> Enum.map(fn {field, message} -> ArgumentError.exception("#{field} #{message}") end)
    |> List.first()
  end

  @spec to_translated_errors(Changeset.t()) :: Keyword.t()
  def to_translated_errors(changeset) do
    changeset
    |> Changeset.traverse_errors(&translate_errors/1)
    |> Enum.map(&extract_errors/1)
    |> List.flatten()
  end

  ## Conversions

  @spec maybe_stringify_list([atom() | String.t()]) :: [String.t()]
  def maybe_stringify_list([head | _] = list) when is_atom(head) do
    for elem <- list, do: to_string(elem)
  end

  def maybe_stringify_list(list), do: list

  @spec maybe_to_atom(atom() | String.t()) :: atom()
  def maybe_to_atom(key) when is_binary(key), do: String.to_existing_atom(key)
  def maybe_to_atom(key), do: key

  # Helpers

  defp translate_errors({msg, opt}) do
    Regex.replace(~r"%{(\w+)}", msg, fn _, key ->
      opt
      |> Keyword.get(String.to_existing_atom(key), key)
      |> to_string()
    end)
  end

  defp extract_errors({_key, val}) when is_map(val) do
    val
    |> Map.to_list()
    |> Enum.map(&extract_errors/1)
  end

  defp extract_errors({key, [message | _]}), do: {key, message}
end

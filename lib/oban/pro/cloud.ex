defmodule Oban.Pro.Cloud do
  @moduledoc """
  A behaviour for interacting with cloud hosting providers.
  """

  @type conf :: term()
  @type opts :: keyword()
  @type quantity :: non_neg_integer()

  @doc """
  Executed once at runtime to gather, normalize, and transform options.
  """
  @callback init(opts()) :: conf()

  @doc """
  Called to horizontally scale a cloud resource up or down.

  Successful scaling requests must return a new `conf` to be used during the next call to
  `scale/2`. That allows cloud modules to track responses for additional control and
  introspection.
  """
  @callback scale(quantity(), conf()) :: {:ok, conf()} | {:error, term()}
end

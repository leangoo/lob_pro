defmodule Oban.Pro.Stage do
  @moduledoc false

  alias Oban.Job

  @type args :: Job.args()
  @type changeset :: Job.changeset()
  @type conf :: term()
  @type error :: Exception.t()
  @type job :: Job.t()
  @type opts :: Keyword.t()
  @type result :: term()

  @doc """
  Initialize configuration passed to various before callbacks.
  """
  @callback init(module(), opts()) :: {:ok, conf()} | {:error, error()}

  @doc """
  Pre-process the args and/or opts for a job before calling `new/2`.
  """
  @callback before_new(args(), opts(), conf()) :: {:ok, args(), opts()} | {:error, error()}

  @doc """
  Pre-process a job before calling `process/1`.
  """
  @callback before_process(job(), conf()) :: {:ok, job()} | {:error, error()}

  @doc """
  Post-process a job before returning from the wrapping `perform/1`
  """
  @callback after_process(result(), job(), conf()) :: :ok

  @optional_callbacks before_new: 3, before_process: 2, after_process: 3
end

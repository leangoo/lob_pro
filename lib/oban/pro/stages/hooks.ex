defmodule Oban.Pro.Stages.Hooks do
  @moduledoc false

  @behaviour Oban.Pro.Stage

  alias Oban.Pro.{Utils, Worker}

  require Logger

  defstruct modules: []

  @impl Oban.Pro.Stage
  def init(worker, modules) do
    with :ok <- Utils.validate_opts(modules, &validate_opt/1) do
      modules =
        if function_exported?(worker, :after_process, 2) do
          [worker | modules]
        else
          modules
        end

      {:ok, struct!(__MODULE__, modules: modules)}
    end
  end

  def attach_hook(module) do
    with :ok <- Utils.validate_opts([module], &validate_opt/1) do
      modules =
        :oban_pro_hooks
        |> :persistent_term.get([])
        |> Enum.concat([module])
        |> Enum.uniq()

      :persistent_term.put(:oban_pro_hooks, modules)
    end
  end

  def detach_hook(module) do
    case :persistent_term.get(:oban_pro_hooks, []) do
      [_ | _] = modules ->
        :persistent_term.put(:oban_pro_hooks, modules -- [module])

      _ ->
        :ok
    end
  end

  def handle_event(_event, _timing, %{job: job, state: state, worker: worker}, _conf) do
    with {:ok, worker} <- Oban.Worker.from_string(worker),
         true <- function_exported?(worker, :__stages__, 0),
         opts = worker.__opts__(),
         {:ok, job} <- Worker.before_process(job, opts) do
      hook_state = exec_to_hook_state(state)

      module_hooks =
        opts
        |> get_in([:stages, __MODULE__, Access.key!(:modules)])
        |> List.wrap()

      global_hooks = :persistent_term.get(:oban_pro_hooks, [])

      (module_hooks ++ global_hooks)
      |> Enum.uniq()
      |> Enum.each(fn module -> module.after_process(hook_state, job) end)
    end
  catch
    kind, value ->
      Logger.error(fn ->
        "[Oban.Pro.Worker] hook error: " <> Exception.format(kind, value)
      end)
  end

  defp exec_to_hook_state(:cancelled), do: :cancel
  defp exec_to_hook_state(:success), do: :complete
  defp exec_to_hook_state(:discard), do: :discard
  defp exec_to_hook_state(:failure), do: :error
  defp exec_to_hook_state(:snoozed), do: :snooze

  # Validation

  defp validate_opt(modules) do
    modules
    |> List.wrap()
    |> Utils.validate_opts(fn module ->
      cond do
        not Code.ensure_loaded?(module) ->
          {:error, "unable to load callback module #{inspect(module)}"}

        not function_exported?(module, :after_process, 2) ->
          {:error, "#{inspect(module)} doesn't implement after_process/2"}

        true ->
          :ok
      end
    end)
  end
end

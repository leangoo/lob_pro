defmodule Oban.Pro.Workers.Chain do
  @moduledoc """
  Chain workers link together to ensure they run in a strict sequential order.

  Jobs in a chain only run after the previous job completes, successfully or otherwise.
  """
end

defmodule Oban.Pro.Migrations.DynamicQueues do
  @moduledoc false

  use Ecto.Migration

  def change(opts \\ []) do
    prefix = Keyword.get(opts, :prefix, "public")

    create table(:oban_queues, primary_key: false, prefix: prefix) do
      add :name, :text, primary_key: true, null: false
      add :opts, :map, null: false, default: %{}
      add :only, :map, null: false, default: %{}
      add :lock_version, :integer, default: 1

      add :inserted_at, :utc_datetime_usec,
        null: false,
        default: fragment("timezone('utc', now())")

      add :updated_at, :utc_datetime_usec,
        null: false,
        default: fragment("timezone('utc', now())")
    end
  end

  def up(opts \\ []) do
    change(opts)
  end

  def down(opts \\ []) do
    prefix = Keyword.get(opts, :prefix, "public")

    drop_if_exists table(:oban_queues, prefix: prefix)
  end
end

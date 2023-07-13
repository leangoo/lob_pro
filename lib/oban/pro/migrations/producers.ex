defmodule Oban.Pro.Migrations.Producers do
  @moduledoc false

  use Ecto.Migration

  def change(opts \\ []) do
    prefix = Keyword.get(opts, :prefix, "public")

    create table(:oban_producers, primary_key: false, prefix: prefix) do
      add :uuid, :uuid, primary_key: true, null: false
      add :name, :text, null: false
      add :node, :text, null: false
      add :queue, :text, null: false
      add :meta, :map, null: false, default: %{}

      add :started_at, :utc_datetime_usec,
        null: false,
        default: fragment("timezone('utc', now())")

      add :updated_at, :utc_datetime_usec,
        null: false,
        default: fragment("timezone('utc', now())")
    end

    execute "ALTER TABLE #{prefix}.oban_producers SET UNLOGGED",
            "ALTER TABLE #{prefix}.oban_producers SET LOGGED"
  end

  def up(opts \\ []) do
    change(opts)
  end

  def down(opts \\ []) do
    prefix = Keyword.get(opts, :prefix, "public")

    drop_if_exists table(:oban_producers, prefix: prefix)
  end
end

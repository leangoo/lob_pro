defmodule Oban.Pro.Migrations.DynamicCron do
  @moduledoc false

  use Ecto.Migration

  def change(opts \\ []) do
    prefix = Keyword.get(opts, :prefix, "public")

    create table(:oban_crons, primary_key: false, prefix: prefix) do
      add :name, :text, primary_key: true, null: false
      add :expression, :text, null: false
      add :worker, :text, null: false
      add :opts, :map, null: false
      add :paused, :boolean, null: false, default: false
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

    drop_if_exists table(:oban_crons, prefix: prefix)
  end
end

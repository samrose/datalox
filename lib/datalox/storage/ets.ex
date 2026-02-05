defmodule Datalox.Storage.ETS do
  @moduledoc """
  ETS-based storage backend for Datalox.

  Uses one ETS table per predicate for efficient lookups.
  Tables are created as bags to allow duplicate-free storage of facts.
  """

  @behaviour Datalox.Storage

  @type state :: %{
          tables: %{atom() => :ets.tab()},
          name: atom()
        }

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    {:ok, %{tables: %{}, name: name}}
  end

  @impl true
  def insert(state, predicate, tuple) do
    state = ensure_table(state, predicate)
    table = state.tables[predicate]
    :ets.insert(table, {tuple})
    {:ok, state}
  end

  @impl true
  def delete(state, predicate, tuple) do
    case Map.get(state.tables, predicate) do
      nil ->
        {:ok, state}

      table ->
        :ets.delete_object(table, {tuple})
        {:ok, state}
    end
  end

  @impl true
  def lookup(state, predicate, pattern) do
    case Map.get(state.tables, predicate) do
      nil ->
        {:ok, []}

      table ->
        results =
          table
          |> :ets.tab2list()
          |> Enum.filter(fn {tuple} -> matches_pattern?(tuple, pattern) end)
          |> Enum.map(fn {tuple} -> tuple end)

        {:ok, results}
    end
  end

  @impl true
  def all(state, predicate) do
    case Map.get(state.tables, predicate) do
      nil ->
        {:ok, []}

      table ->
        results = :ets.tab2list(table) |> Enum.map(fn {tuple} -> tuple end)
        {:ok, results}
    end
  end

  @impl true
  def terminate(state) do
    Enum.each(state.tables, fn {_pred, table} ->
      :ets.delete(table)
    end)

    :ok
  end

  # Ensure a table exists for the given predicate
  defp ensure_table(state, predicate) do
    if Map.has_key?(state.tables, predicate) do
      state
    else
      table_name = :"#{state.name}_#{predicate}"
      table = :ets.new(table_name, [:set, :protected])
      put_in(state.tables[predicate], table)
    end
  end

  # Check if a tuple matches a pattern (with :_ wildcards)
  defp matches_pattern?(tuple, pattern) when length(tuple) == length(pattern) do
    Enum.zip(tuple, pattern)
    |> Enum.all?(fn
      {_, :_} -> true
      {a, a} -> true
      {_, _} -> false
    end)
  end

  defp matches_pattern?(_, _), do: false
end

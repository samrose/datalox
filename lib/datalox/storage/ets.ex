defmodule Datalox.Storage.ETS do
  @moduledoc """
  ETS-based storage backend for Datalox.

  Uses one ETS `:duplicate_bag` table per predicate, keyed by the first
  element of each tuple for O(1) first-column lookups.
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
    key = first_key(tuple)
    :ets.insert(table, {key, tuple})
    {:ok, state}
  end

  @impl true
  def delete(state, predicate, tuple) do
    case Map.get(state.tables, predicate) do
      nil ->
        {:ok, state}

      table ->
        key = first_key(tuple)
        :ets.delete_object(table, {key, tuple})
        {:ok, state}
    end
  end

  @impl true
  def lookup(state, predicate, pattern) do
    case Map.get(state.tables, predicate) do
      nil ->
        {:ok, []}

      table ->
        results = indexed_lookup(table, pattern)
        {:ok, results}
    end
  end

  @impl true
  def all(state, predicate) do
    case Map.get(state.tables, predicate) do
      nil ->
        {:ok, []}

      table ->
        results = :ets.tab2list(table) |> Enum.map(fn {_key, tuple} -> tuple end)
        {:ok, results}
    end
  end

  @impl true
  def count(state, predicate) do
    case Map.get(state.tables, predicate) do
      nil -> {:ok, 0}
      table -> {:ok, :ets.info(table, :size)}
    end
  end

  @impl true
  def terminate(state) do
    Enum.each(state.tables, fn {_pred, table} ->
      :ets.delete(table)
    end)

    :ok
  end

  # --- Private ---

  defp ensure_table(state, predicate) do
    if Map.has_key?(state.tables, predicate) do
      state
    else
      table_name = :"#{state.name}_#{predicate}"
      table = :ets.new(table_name, [:duplicate_bag, :protected])
      put_in(state.tables[predicate], table)
    end
  end

  # When the first element of the pattern is bound, use ETS key lookup
  defp indexed_lookup(table, [first | rest_pattern]) when first != :_ do
    :ets.lookup(table, first)
    |> Enum.filter(fn {_key, tuple} -> matches_pattern?(tuple, [first | rest_pattern]) end)
    |> Enum.map(fn {_key, tuple} -> tuple end)
  end

  # When the first element is a wildcard, fall back to full scan
  defp indexed_lookup(table, pattern) do
    :ets.tab2list(table)
    |> Enum.filter(fn {_key, tuple} -> matches_pattern?(tuple, pattern) end)
    |> Enum.map(fn {_key, tuple} -> tuple end)
  end

  defp matches_pattern?(tuple, pattern) when length(tuple) == length(pattern) do
    Enum.zip(tuple, pattern)
    |> Enum.all?(fn
      {_, :_} -> true
      {a, a} -> true
      {_, _} -> false
    end)
  end

  defp matches_pattern?(_, _), do: false

  defp first_key([first | _]), do: first
  defp first_key([]), do: :__empty__
end

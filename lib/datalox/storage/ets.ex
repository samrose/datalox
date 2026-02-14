defmodule Datalox.Storage.ETS do
  @moduledoc """
  ETS-based storage backend for Datalox.

  Uses one ETS `:duplicate_bag` table per predicate, keyed by the first
  element of each tuple for O(1) first-column lookups. Supports optional
  secondary indexes on other columns via `create_index/3`.
  """

  @behaviour Datalox.Storage

  @type state :: %{
          tables: %{atom() => :ets.tab()},
          indexes: %{{atom(), non_neg_integer()} => :ets.tab()},
          name: atom()
        }

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    {:ok, %{tables: %{}, indexes: %{}, name: name}}
  end

  @impl true
  def insert(state, predicate, tuple) do
    state = ensure_table(state, predicate)
    table = state.tables[predicate]
    key = first_key(tuple)

    # Deduplicate: check if this exact fact already exists
    existing = :ets.lookup(table, key)
    already_exists = Enum.any?(existing, fn {_k, t} -> t == tuple end)

    if already_exists do
      {:ok, state}
    else
      :ets.insert(table, {key, tuple})
      update_indexes_on_insert(state, predicate, tuple)
      {:ok, state}
    end
  end

  @impl true
  def delete(state, predicate, tuple) do
    case Map.get(state.tables, predicate) do
      nil ->
        {:ok, state}

      table ->
        key = first_key(tuple)
        :ets.delete_object(table, {key, tuple})

        # Update secondary indexes
        state = update_indexes_on_delete(state, predicate, tuple)

        {:ok, state}
    end
  end

  @impl true
  def lookup(state, predicate, pattern) do
    case Map.get(state.tables, predicate) do
      nil ->
        {:ok, []}

      table ->
        results = indexed_lookup(state, table, predicate, pattern)
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
  def all_predicates(state) do
    {:ok, Map.keys(state.tables)}
  end

  @doc """
  Create a secondary index on a column for faster lookups.
  Backfills existing data into the index.
  """
  @impl true
  def create_index(state, predicate, column) do
    state = ensure_table(state, predicate)
    idx_key = {predicate, column}

    if Map.has_key?(state.indexes, idx_key) do
      {:ok, state}
    else
      idx_table =
        :ets.new(:"#{state.name}_#{predicate}_idx_#{column}", [:duplicate_bag, :public])

      # Backfill existing data
      primary = state.tables[predicate]

      :ets.tab2list(primary)
      |> Enum.each(fn {_key, tuple} ->
        col_val = Enum.at(tuple, column)
        :ets.insert(idx_table, {col_val, tuple})
      end)

      {:ok, put_in(state.indexes[idx_key], idx_table)}
    end
  end

  @doc """
  Run an ETS match spec against the table for a predicate.
  """
  def select(state, predicate, match_spec) do
    case Map.get(state.tables, predicate) do
      nil -> {:ok, []}
      table -> {:ok, :ets.select(table, match_spec) |> Enum.map(fn {_key, tuple} -> tuple end)}
    end
  end

  @impl true
  def terminate(state) do
    Enum.each(state.tables, fn {_pred, table} ->
      :ets.delete(table)
    end)

    Enum.each(state.indexes, fn {_key, table} ->
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
      table = :ets.new(table_name, [:duplicate_bag, :public])
      put_in(state.tables[predicate], table)
    end
  end

  defp update_indexes_on_insert(state, predicate, tuple) do
    state.indexes
    |> Enum.filter(fn {{pred, _col}, _tab} -> pred == predicate end)
    |> Enum.each(fn {{_pred, col}, idx_table} ->
      col_val = Enum.at(tuple, col)
      :ets.insert(idx_table, {col_val, tuple})
    end)

    state
  end

  defp update_indexes_on_delete(state, predicate, tuple) do
    state.indexes
    |> Enum.filter(fn {{pred, _col}, _tab} -> pred == predicate end)
    |> Enum.each(fn {{_pred, col}, idx_table} ->
      col_val = Enum.at(tuple, col)
      :ets.delete_object(idx_table, {col_val, tuple})
    end)

    state
  end

  # Find the best index for the pattern and use it
  defp indexed_lookup(state, table, predicate, pattern) do
    case find_best_index(state, predicate, pattern) do
      {:index, col, idx_table} ->
        col_val = Enum.at(pattern, col)

        :ets.lookup(idx_table, col_val)
        |> Enum.filter(fn {_key, tuple} -> matches_pattern?(tuple, pattern) end)
        |> Enum.map(fn {_key, tuple} -> tuple end)

      :first_column ->
        [first | _] = pattern

        :ets.lookup(table, first)
        |> Enum.filter(fn {_key, tuple} -> matches_pattern?(tuple, pattern) end)
        |> Enum.map(fn {_key, tuple} -> tuple end)

      :full_scan ->
        :ets.tab2list(table)
        |> Enum.filter(fn {_key, tuple} -> matches_pattern?(tuple, pattern) end)
        |> Enum.map(fn {_key, tuple} -> tuple end)
    end
  end

  defp find_best_index(_state, _predicate, [first | _rest]) when first != :_ do
    # First column is bound — use primary key lookup
    :first_column
  end

  defp find_best_index(state, predicate, pattern) do
    # First column is wildcard — look for secondary indexes on bound columns
    bound_columns =
      pattern
      |> Enum.with_index()
      |> Enum.reject(fn {val, _idx} -> val == :_ end)
      |> Enum.map(fn {_val, idx} -> idx end)

    case Enum.find(bound_columns, fn col -> Map.has_key?(state.indexes, {predicate, col}) end) do
      nil -> :full_scan
      col -> {:index, col, state.indexes[{predicate, col}]}
    end
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

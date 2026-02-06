defmodule Datalox.Storage.CubDB do
  @moduledoc """
  CubDB-backed persistent storage backend for Datalox.

  Uses CubDB for durable persistence of facts to disk.
  Keys are `{predicate, tuple}` and values are `true`.
  """

  @behaviour Datalox.Storage

  @impl true
  def init(opts) do
    data_dir = Keyword.fetch!(opts, :data_dir)
    File.mkdir_p!(data_dir)
    {:ok, db} = CubDB.start_link(data_dir: data_dir)
    {:ok, %{db: db, name: Keyword.get(opts, :name, :cubdb)}}
  end

  @impl true
  def insert(state, predicate, tuple) do
    key = {predicate, tuple}
    CubDB.put(state.db, key, true)
    {:ok, state}
  end

  @impl true
  def delete(state, predicate, tuple) do
    key = {predicate, tuple}
    CubDB.delete(state.db, key)
    {:ok, state}
  end

  @impl true
  def lookup(state, predicate, pattern) do
    results =
      CubDB.select(state.db,
        min_key: {predicate, :__min__},
        max_key: {predicate, <<255>>}
      )
      |> Enum.filter(fn
        {{^predicate, tuple}, _} -> matches?(tuple, pattern)
        _ -> false
      end)
      |> Enum.map(fn {{_, tuple}, _} -> tuple end)

    {:ok, results}
  end

  @impl true
  def all(state, predicate) do
    results =
      CubDB.select(state.db,
        min_key: {predicate, :__min__},
        max_key: {predicate, <<255>>}
      )
      |> Enum.filter(fn {{pred, _}, _} -> pred == predicate end)
      |> Enum.map(fn {{_, tuple}, _} -> tuple end)

    {:ok, results}
  end

  @impl true
  def count(state, predicate) do
    {:ok, results} = all(state, predicate)
    {:ok, length(results)}
  end

  @impl true
  def terminate(state) do
    GenServer.stop(state.db)
    :ok
  end

  defp matches?(tuple, pattern) when is_list(tuple) and is_list(pattern) do
    length(tuple) == length(pattern) and
      Enum.zip(tuple, pattern)
      |> Enum.all?(fn
        {_, :_} -> true
        {a, a} -> true
        _ -> false
      end)
  end

  defp matches?(_, _), do: false
end

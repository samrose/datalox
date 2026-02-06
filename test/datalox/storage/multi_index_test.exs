defmodule Datalox.Storage.MultiIndexTest do
  use ExUnit.Case, async: true
  alias Datalox.Storage.ETS

  setup do
    {:ok, st} = ETS.init(name: :"multi_idx_#{:erlang.unique_integer()}")
    on_exit(fn -> ETS.terminate(st) end)
    {:ok, st: st}
  end

  test "lookup on second column uses index when available", %{st: st} do
    {:ok, st} = ETS.insert(st, :edge, [1, "a"])
    {:ok, st} = ETS.insert(st, :edge, [2, "a"])
    {:ok, st} = ETS.insert(st, :edge, [3, "b"])

    {:ok, st} = ETS.create_index(st, :edge, 1)

    {:ok, results} = ETS.lookup(st, :edge, [:_, "a"])
    assert length(results) == 2
    assert [1, "a"] in results
    assert [2, "a"] in results
  end

  test "insert maintains secondary indexes", %{st: st} do
    {:ok, st} = ETS.create_index(st, :edge, 1)
    {:ok, st} = ETS.insert(st, :edge, [1, "x"])
    {:ok, st} = ETS.insert(st, :edge, [2, "x"])

    {:ok, results} = ETS.lookup(st, :edge, [:_, "x"])
    assert length(results) == 2
  end

  test "delete maintains secondary indexes", %{st: st} do
    {:ok, st} = ETS.create_index(st, :edge, 1)
    {:ok, st} = ETS.insert(st, :edge, [1, "x"])
    {:ok, st} = ETS.delete(st, :edge, [1, "x"])

    {:ok, results} = ETS.lookup(st, :edge, [:_, "x"])
    assert results == []
  end

  test "multiple indexes on different columns", %{st: st} do
    {:ok, st} = ETS.create_index(st, :triple, 1)
    {:ok, st} = ETS.create_index(st, :triple, 2)
    {:ok, st} = ETS.insert(st, :triple, ["a", "b", "c"])
    {:ok, st} = ETS.insert(st, :triple, ["d", "b", "e"])
    {:ok, st} = ETS.insert(st, :triple, ["f", "g", "c"])

    {:ok, results} = ETS.lookup(st, :triple, [:_, "b", :_])
    assert length(results) == 2
    assert ["a", "b", "c"] in results
    assert ["d", "b", "e"] in results

    {:ok, results} = ETS.lookup(st, :triple, [:_, :_, "c"])
    assert length(results) == 2
    assert ["a", "b", "c"] in results
    assert ["f", "g", "c"] in results
  end

  test "creating duplicate index is idempotent", %{st: st} do
    {:ok, st} = ETS.create_index(st, :edge, 1)
    {:ok, st2} = ETS.create_index(st, :edge, 1)
    assert st == st2
  end
end

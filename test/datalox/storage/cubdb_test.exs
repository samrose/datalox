defmodule Datalox.Storage.CubDBTest do
  use ExUnit.Case, async: false
  alias Datalox.Storage.CubDB, as: CubStorage

  setup do
    test_dir = "/tmp/datalox_cubdb_test_#{:erlang.unique_integer([:positive])}"
    File.rm_rf!(test_dir)
    {:ok, st} = CubStorage.init(name: :test_cubdb, data_dir: test_dir)

    on_exit(fn ->
      try do
        CubStorage.terminate(st)
      catch
        :exit, _ -> :ok
      end

      File.rm_rf!(test_dir)
    end)

    {:ok, st: st, test_dir: test_dir}
  end

  test "insert and lookup", %{st: st} do
    {:ok, st} = CubStorage.insert(st, :user, ["alice", :admin])
    {:ok, results} = CubStorage.lookup(st, :user, ["alice", :_])
    assert ["alice", :admin] in results
  end

  test "lookup with all wildcards", %{st: st} do
    {:ok, st} = CubStorage.insert(st, :user, ["alice", :admin])
    {:ok, st} = CubStorage.insert(st, :user, ["bob", :viewer])
    {:ok, results} = CubStorage.lookup(st, :user, [:_, :_])
    assert length(results) == 2
  end

  test "lookup with exact match", %{st: st} do
    {:ok, st} = CubStorage.insert(st, :user, ["alice", :admin])
    {:ok, st} = CubStorage.insert(st, :user, ["bob", :viewer])
    {:ok, results} = CubStorage.lookup(st, :user, ["alice", :admin])
    assert results == [["alice", :admin]]
  end

  test "delete removes fact", %{st: st} do
    {:ok, st} = CubStorage.insert(st, :user, ["alice", :admin])
    {:ok, st} = CubStorage.delete(st, :user, ["alice", :admin])
    {:ok, results} = CubStorage.lookup(st, :user, ["alice", :_])
    assert results == []
  end

  test "all returns all facts for predicate", %{st: st} do
    {:ok, st} = CubStorage.insert(st, :user, ["alice"])
    {:ok, st} = CubStorage.insert(st, :user, ["bob"])
    {:ok, st} = CubStorage.insert(st, :other, ["xyz"])
    {:ok, results} = CubStorage.all(st, :user)
    assert length(results) == 2
  end

  test "count returns correct count", %{st: st} do
    {:ok, st} = CubStorage.insert(st, :fact, ["a"])
    {:ok, st} = CubStorage.insert(st, :fact, ["b"])
    {:ok, count} = CubStorage.count(st, :fact)
    assert count == 2
  end

  test "data persists across restarts", %{st: st, test_dir: test_dir} do
    {:ok, _st} = CubStorage.insert(st, :fact, ["hello"])
    CubStorage.terminate(st)

    {:ok, st2} = CubStorage.init(name: :test_cubdb2, data_dir: test_dir)
    {:ok, results} = CubStorage.lookup(st2, :fact, ["hello"])
    assert ["hello"] in results
    CubStorage.terminate(st2)
  end

  test "different predicates are isolated", %{st: st} do
    {:ok, st} = CubStorage.insert(st, :users, ["alice"])
    {:ok, st} = CubStorage.insert(st, :items, ["widget"])
    {:ok, users} = CubStorage.all(st, :users)
    {:ok, items} = CubStorage.all(st, :items)
    assert users == [["alice"]]
    assert items == [["widget"]]
  end
end

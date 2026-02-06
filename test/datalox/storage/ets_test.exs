defmodule Datalox.Storage.ETSTest do
  use ExUnit.Case, async: true

  alias Datalox.Storage.ETS

  setup do
    {:ok, state} = ETS.init(name: :test_storage)
    on_exit(fn -> ETS.terminate(state) end)
    {:ok, state: state}
  end

  describe "insert/3 and lookup/3" do
    test "inserts and retrieves a fact", %{state: state} do
      {:ok, state} = ETS.insert(state, :user, ["alice", :admin, "engineering"])
      {:ok, results} = ETS.lookup(state, :user, [:_, :_, :_])

      assert results == [["alice", :admin, "engineering"]]
    end

    test "handles multiple facts for same predicate", %{state: state} do
      {:ok, state} = ETS.insert(state, :user, ["alice", :admin, "engineering"])
      {:ok, state} = ETS.insert(state, :user, ["bob", :viewer, "sales"])
      {:ok, results} = ETS.lookup(state, :user, [:_, :_, :_])

      assert length(results) == 2
      assert ["alice", :admin, "engineering"] in results
      assert ["bob", :viewer, "sales"] in results
    end

    test "filters by pattern", %{state: state} do
      {:ok, state} = ETS.insert(state, :user, ["alice", :admin, "engineering"])
      {:ok, state} = ETS.insert(state, :user, ["bob", :viewer, "sales"])
      {:ok, results} = ETS.lookup(state, :user, [:_, :admin, :_])

      assert results == [["alice", :admin, "engineering"]]
    end

    test "returns empty list for no matches", %{state: state} do
      {:ok, state} = ETS.insert(state, :user, ["alice", :admin, "engineering"])
      {:ok, results} = ETS.lookup(state, :user, [:_, :nobody, :_])

      assert results == []
    end
  end

  describe "delete/3" do
    test "removes a fact", %{state: state} do
      {:ok, state} = ETS.insert(state, :user, ["alice", :admin, "engineering"])
      {:ok, state} = ETS.delete(state, :user, ["alice", :admin, "engineering"])
      {:ok, results} = ETS.lookup(state, :user, [:_, :_, :_])

      assert results == []
    end

    test "only removes exact match", %{state: state} do
      {:ok, state} = ETS.insert(state, :user, ["alice", :admin, "engineering"])
      {:ok, state} = ETS.insert(state, :user, ["bob", :viewer, "sales"])
      {:ok, state} = ETS.delete(state, :user, ["alice", :admin, "engineering"])
      {:ok, results} = ETS.lookup(state, :user, [:_, :_, :_])

      assert results == [["bob", :viewer, "sales"]]
    end
  end

  describe "all/2" do
    test "returns all facts for a predicate", %{state: state} do
      {:ok, state} = ETS.insert(state, :user, ["alice", :admin, "engineering"])
      {:ok, state} = ETS.insert(state, :user, ["bob", :viewer, "sales"])
      {:ok, state} = ETS.insert(state, :role, [:admin, :write])
      {:ok, results} = ETS.all(state, :user)

      assert length(results) == 2
    end

    test "returns empty list for unknown predicate", %{state: state} do
      {:ok, results} = ETS.all(state, :unknown)
      assert results == []
    end
  end

  describe "indexed lookup" do
    test "lookup with bound first column uses index", %{state: state} do
      # Insert many facts
      state =
        Enum.reduce(1..100, state, fn i, st ->
          {:ok, st} = ETS.insert(st, :node, [i, "label_#{i}"])
          st
        end)

      # Lookup by first column should return exactly one result
      {:ok, results} = ETS.lookup(state, :node, [42, :_])
      assert results == [[42, "label_42"]]
    end

    test "lookup with all wildcards returns all facts", %{state: state} do
      {:ok, state} = ETS.insert(state, :edge, [1, 2])
      {:ok, state} = ETS.insert(state, :edge, [2, 3])
      {:ok, state} = ETS.insert(state, :edge, [1, 3])
      {:ok, results} = ETS.lookup(state, :edge, [:_, :_])
      assert length(results) == 3
    end

    test "lookup with bound non-first column still works", %{state: state} do
      {:ok, state} = ETS.insert(state, :user, ["alice", :admin])
      {:ok, state} = ETS.insert(state, :user, ["bob", :viewer])
      {:ok, state} = ETS.insert(state, :user, ["carol", :admin])
      {:ok, results} = ETS.lookup(state, :user, [:_, :admin])
      assert length(results) == 2
    end
  end

  describe "count/2" do
    test "returns 0 for unknown predicate", %{state: state} do
      assert {:ok, 0} == ETS.count(state, :unknown)
    end

    test "returns number of facts for predicate", %{state: state} do
      {:ok, state} = ETS.insert(state, :user, ["alice", :admin])
      {:ok, state} = ETS.insert(state, :user, ["bob", :viewer])
      {:ok, state} = ETS.insert(state, :role, [:admin, :write])
      assert {:ok, 2} == ETS.count(state, :user)
      assert {:ok, 1} == ETS.count(state, :role)
    end
  end
end

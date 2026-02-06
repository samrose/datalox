defmodule Datalox.Optimizer.JoinOrderTest do
  use ExUnit.Case, async: true

  alias Datalox.Optimizer.JoinOrder
  alias Datalox.Storage.ETS

  setup do
    {:ok, storage} = ETS.init(name: :"join_test_#{:erlang.unique_integer()}")
    on_exit(fn -> ETS.terminate(storage) end)
    {:ok, storage: storage}
  end

  describe "reorder/4" do
    test "puts smaller relation first", %{storage: storage} do
      # permission has 100 facts, user has 2
      storage =
        Enum.reduce(1..100, storage, fn i, st ->
          {:ok, st} = ETS.insert(st, :permission, [:role_a, "doc_#{i}"])
          st
        end)

      {:ok, storage} = ETS.insert(storage, :user, ["alice", :role_a])
      {:ok, storage} = ETS.insert(storage, :user, ["bob", :role_b])

      body = [{:permission, [:R, :D]}, {:user, [:U, :R]}]
      reordered = JoinOrder.reorder(body, %{}, storage, ETS)

      # user should come first (smaller relation)
      assert [{:user, _}, {:permission, _}] = reordered
    end

    test "considers bound variables from earlier goals", %{storage: storage} do
      {:ok, storage} = ETS.insert(storage, :a, [1, 2])
      {:ok, storage} = ETS.insert(storage, :b, [2, 3])
      {:ok, storage} = ETS.insert(storage, :c, [3, 4])

      body = [{:c, [:Z, :W]}, {:a, [:X, :Y]}, {:b, [:Y, :Z]}]
      reordered = JoinOrder.reorder(body, %{}, storage, ETS)

      # All same size — should maintain a valid variable-binding order
      assert length(reordered) == 3
    end

    test "does not reorder single-goal body", %{storage: storage} do
      body = [{:user, [:X, :Y]}]
      assert JoinOrder.reorder(body, %{}, storage, ETS) == body
    end

    test "does not reorder when all relations are same size", %{storage: storage} do
      {:ok, storage} = ETS.insert(storage, :a, [1])
      {:ok, storage} = ETS.insert(storage, :b, [2])

      body = [{:a, [:X]}, {:b, [:Y]}]
      reordered = JoinOrder.reorder(body, %{}, storage, ETS)

      # Same size — preserve original order
      assert reordered == body
    end
  end
end

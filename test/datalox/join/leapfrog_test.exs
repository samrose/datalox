defmodule Datalox.Join.LeapfrogTest do
  use ExUnit.Case, async: true
  alias Datalox.{Evaluator, Rule}
  alias Datalox.Join.Leapfrog
  alias Datalox.Storage.ETS

  setup do
    {:ok, storage} = ETS.init(name: :"leapfrog_#{:erlang.unique_integer()}")
    on_exit(fn -> ETS.terminate(storage) end)
    {:ok, storage: storage}
  end

  test "intersects sorted iterators" do
    r = :gb_trees.from_orddict([{1, true}, {2, true}, {3, true}])
    s = :gb_trees.from_orddict([{2, true}, {3, true}, {5, true}])
    t = :gb_trees.from_orddict([{3, true}, {5, true}, {7, true}])

    result = Leapfrog.intersect([r, s, t])
    assert result == [3]
  end

  test "empty intersection" do
    r = :gb_trees.from_orddict([{1, true}, {2, true}])
    s = :gb_trees.from_orddict([{3, true}, {4, true}])

    assert Leapfrog.intersect([r, s]) == []
  end

  test "full overlap" do
    r = :gb_trees.from_orddict([{1, true}, {2, true}, {3, true}])
    s = :gb_trees.from_orddict([{1, true}, {2, true}, {3, true}])

    assert Leapfrog.intersect([r, s]) == [1, 2, 3]
  end

  test "single element intersection" do
    r = :gb_trees.from_orddict([{5, true}])
    s = :gb_trees.from_orddict([{5, true}])
    t = :gb_trees.from_orddict([{5, true}])

    assert Leapfrog.intersect([r, s, t]) == [5]
  end

  test "triangle query via evaluator", %{storage: storage} do
    # edges: 1->2, 2->3, 1->3 (triangle)
    {:ok, storage} = ETS.insert(storage, :edge, [1, 2])
    {:ok, storage} = ETS.insert(storage, :edge, [2, 3])
    {:ok, storage} = ETS.insert(storage, :edge, [1, 3])

    # triangle(A,B,C) :- edge(A,B), edge(B,C), edge(A,C)
    rules = [
      Rule.new(
        {:triangle, [:A, :B, :C]},
        [{:edge, [:A, :B]}, {:edge, [:B, :C]}, {:edge, [:A, :C]}]
      )
    ]

    {:ok, derived, _} = Evaluator.evaluate(rules, storage, ETS)
    assert {:triangle, [1, 2, 3]} in derived
  end

  test "eligible? detects multi-way join on shared variables" do
    body = [{:edge, [:A, :B]}, {:edge, [:B, :C]}, {:edge, [:A, :C]}]
    assert Leapfrog.eligible?(body)
  end

  test "eligible? rejects two-way join" do
    body = [{:edge, [:A, :B]}, {:edge, [:B, :C]}]
    refute Leapfrog.eligible?(body)
  end
end

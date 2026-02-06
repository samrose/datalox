defmodule Datalox.LatticeTest do
  use ExUnit.Case, async: true
  alias Datalox.{Evaluator, Rule}
  alias Datalox.Lattice.{Max, Min, Set}
  alias Datalox.Storage.ETS

  setup do
    {:ok, storage} = ETS.init(name: :"lattice_#{:erlang.unique_integer()}")
    on_exit(fn -> ETS.terminate(storage) end)
    {:ok, storage: storage}
  end

  test "shortest path with Min lattice", %{storage: storage} do
    {:ok, storage} = ETS.insert(storage, :edge, ["a", "b", 1])
    {:ok, storage} = ETS.insert(storage, :edge, ["b", "c", 2])
    {:ok, storage} = ETS.insert(storage, :edge, ["a", "c", 10])

    # sp(X,Y,D) :- edge(X,Y,D)
    # sp(X,Z,D1+D2) :- sp(X,Y,D1), edge(Y,Z,D2)
    rules = [
      Rule.new({:sp, [:X, :Y, :D]}, [{:edge, [:X, :Y, :D]}]),
      Rule.new(
        {:sp, [:X, :Z, :Total]},
        [{:sp, [:X, :Y, :D1]}, {:edge, [:Y, :Z, :D2]}],
        guards: [{:=, :Total, {:+, :D1, :D2}}]
      )
    ]

    lattice_config = %{
      sp: %{key_columns: [0, 1], lattice_column: 2, lattice: Min}
    }

    {:ok, derived, _} = Evaluator.evaluate(rules, storage, ETS, lattice_config)

    sp_ac = Enum.find(derived, fn {pred, [from, to, _]} -> pred == :sp and from == "a" and to == "c" end)
    assert {:sp, ["a", "c", 3]} = sp_ac
  end

  test "Max lattice keeps largest value", %{storage: storage} do
    {:ok, storage} = ETS.insert(storage, :score, ["alice", 50])
    {:ok, storage} = ETS.insert(storage, :score, ["alice", 90])
    {:ok, storage} = ETS.insert(storage, :score, ["bob", 70])

    rules = [
      Rule.new({:best, [:Name, :S]}, [{:score, [:Name, :S]}])
    ]

    lattice_config = %{
      best: %{key_columns: [0], lattice_column: 1, lattice: Max}
    }

    {:ok, derived, _} = Evaluator.evaluate(rules, storage, ETS, lattice_config)

    alice = Enum.find(derived, fn {_, [name, _]} -> name == "alice" end)
    assert {:best, ["alice", 90]} = alice

    bob = Enum.find(derived, fn {_, [name, _]} -> name == "bob" end)
    assert {:best, ["bob", 70]} = bob
  end

  test "without lattice config, keeps all facts", %{storage: storage} do
    {:ok, storage} = ETS.insert(storage, :score, ["alice", 50])
    {:ok, storage} = ETS.insert(storage, :score, ["alice", 90])

    rules = [
      Rule.new({:result, [:Name, :S]}, [{:score, [:Name, :S]}])
    ]

    {:ok, derived, _} = Evaluator.evaluate(rules, storage, ETS)
    alice_results = Enum.filter(derived, fn {_, [name, _]} -> name == "alice" end)
    assert length(alice_results) == 2
  end

  test "lattice behaviour implementations" do
    assert Min.join(3, 5) == 3
    assert Min.join(5, 3) == 3
    assert Min.bottom() == :infinity

    assert Max.join(3, 5) == 5
    assert Max.join(5, 3) == 5
    assert Max.bottom() == :neg_infinity

    assert Set.join(MapSet.new([1, 2]), MapSet.new([2, 3])) ==
             MapSet.new([1, 2, 3])

    assert Set.bottom() == MapSet.new()
  end
end

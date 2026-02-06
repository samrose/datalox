defmodule Datalox.GuardsTest do
  use ExUnit.Case, async: true
  alias Datalox.{Evaluator, Rule}
  alias Datalox.Storage.ETS

  setup do
    {:ok, storage} = ETS.init(name: :"guards_#{:erlang.unique_integer()}")
    on_exit(fn -> ETS.terminate(storage) end)
    {:ok, storage: storage}
  end

  test "greater-than guard filters results", %{storage: storage} do
    {:ok, storage} = ETS.insert(storage, :score, ["alice", 90])
    {:ok, storage} = ETS.insert(storage, :score, ["bob", 40])
    {:ok, storage} = ETS.insert(storage, :score, ["carol", 70])

    rules = [
      Rule.new({:high_score, [:Name, :S]}, [{:score, [:Name, :S]}],
        guards: [{:>, :S, 50}]
      )
    ]

    {:ok, derived, _} = Evaluator.evaluate(rules, storage, ETS)
    assert {:high_score, ["alice", 90]} in derived
    assert {:high_score, ["carol", 70]} in derived
    refute Enum.any?(derived, fn {_, [n, _]} -> n == "bob" end)
  end

  test "arithmetic assignment guard", %{storage: storage} do
    {:ok, storage} = ETS.insert(storage, :price, ["widget", 100])

    rules = [
      Rule.new({:with_tax, [:Item, :Total]}, [{:price, [:Item, :P]}],
        guards: [{:=, :Total, {:*, :P, 1.1}}]
      )
    ]

    {:ok, derived, _} = Evaluator.evaluate(rules, storage, ETS)
    assert {:with_tax, ["widget", 110.00000000000001]} in derived
  end

  test "not-equal guard", %{storage: storage} do
    {:ok, storage} = ETS.insert(storage, :pair, ["a", "b"])
    {:ok, storage} = ETS.insert(storage, :pair, ["a", "a"])

    rules = [
      Rule.new({:diff_pair, [:X, :Y]}, [{:pair, [:X, :Y]}],
        guards: [{:!=, :X, :Y}]
      )
    ]

    {:ok, derived, _} = Evaluator.evaluate(rules, storage, ETS)
    assert {:diff_pair, ["a", "b"]} in derived
    refute {:diff_pair, ["a", "a"]} in derived
  end

  test "less-than guard", %{storage: storage} do
    {:ok, storage} = ETS.insert(storage, :value, [1])
    {:ok, storage} = ETS.insert(storage, :value, [5])
    {:ok, storage} = ETS.insert(storage, :value, [10])

    rules = [
      Rule.new({:small, [:V]}, [{:value, [:V]}],
        guards: [{:<, :V, 6}]
      )
    ]

    {:ok, derived, _} = Evaluator.evaluate(rules, storage, ETS)
    assert {:small, [1]} in derived
    assert {:small, [5]} in derived
    refute {:small, [10]} in derived
  end

  test "greater-or-equal guard", %{storage: storage} do
    {:ok, storage} = ETS.insert(storage, :value, [5])
    {:ok, storage} = ETS.insert(storage, :value, [10])
    {:ok, storage} = ETS.insert(storage, :value, [3])

    rules = [
      Rule.new({:gte5, [:V]}, [{:value, [:V]}],
        guards: [{:>=, :V, 5}]
      )
    ]

    {:ok, derived, _} = Evaluator.evaluate(rules, storage, ETS)
    assert {:gte5, [5]} in derived
    assert {:gte5, [10]} in derived
    refute {:gte5, [3]} in derived
  end

  test "less-or-equal guard", %{storage: storage} do
    {:ok, storage} = ETS.insert(storage, :value, [5])
    {:ok, storage} = ETS.insert(storage, :value, [10])
    {:ok, storage} = ETS.insert(storage, :value, [3])

    rules = [
      Rule.new({:lte5, [:V]}, [{:value, [:V]}],
        guards: [{:<=, :V, 5}]
      )
    ]

    {:ok, derived, _} = Evaluator.evaluate(rules, storage, ETS)
    assert {:lte5, [5]} in derived
    assert {:lte5, [3]} in derived
    refute {:lte5, [10]} in derived
  end

  test "arithmetic addition in assignment", %{storage: storage} do
    {:ok, storage} = ETS.insert(storage, :score, ["a", 10])
    {:ok, storage} = ETS.insert(storage, :bonus, ["a", 5])

    rules = [
      Rule.new({:total, [:Name, :T]}, [{:score, [:Name, :S]}, {:bonus, [:Name, :B]}],
        guards: [{:=, :T, {:+, :S, :B}}]
      )
    ]

    {:ok, derived, _} = Evaluator.evaluate(rules, storage, ETS)
    assert {:total, ["a", 15]} in derived
  end

  test "multiple guards combined", %{storage: storage} do
    {:ok, storage} = ETS.insert(storage, :score, ["alice", 90])
    {:ok, storage} = ETS.insert(storage, :score, ["bob", 40])
    {:ok, storage} = ETS.insert(storage, :score, ["carol", 70])

    rules = [
      Rule.new({:mid_score, [:Name, :S]}, [{:score, [:Name, :S]}],
        guards: [{:>, :S, 50}, {:<, :S, 80}]
      )
    ]

    {:ok, derived, _} = Evaluator.evaluate(rules, storage, ETS)
    assert {:mid_score, ["carol", 70]} in derived
    refute Enum.any?(derived, fn {_, [n, _]} -> n == "alice" end)
    refute Enum.any?(derived, fn {_, [n, _]} -> n == "bob" end)
  end
end

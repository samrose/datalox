defmodule Datalox.EvaluatorTest do
  use ExUnit.Case, async: true

  alias Datalox.Evaluator
  alias Datalox.Rule
  alias Datalox.Storage.ETS

  setup do
    {:ok, storage} = ETS.init(name: :"test_#{:erlang.unique_integer()}")
    on_exit(fn -> ETS.terminate(storage) end)
    {:ok, storage: storage}
  end

  describe "evaluate/3 with simple rules" do
    test "derives facts from single rule", %{storage: storage} do
      # Facts: parent(alice, bob), parent(bob, carol)
      {:ok, storage} = ETS.insert(storage, :parent, ["alice", "bob"])
      {:ok, storage} = ETS.insert(storage, :parent, ["bob", "carol"])

      # Rule: ancestor(X, Y) :- parent(X, Y)
      rules = [
        Rule.new({:ancestor, [:X, :Y]}, [{:parent, [:X, :Y]}])
      ]

      {:ok, derived, _storage} = Evaluator.evaluate(rules, storage, ETS)

      assert {:ancestor, ["alice", "bob"]} in derived
      assert {:ancestor, ["bob", "carol"]} in derived
    end

    test "handles recursive rules", %{storage: storage} do
      # Facts: parent(alice, bob), parent(bob, carol)
      {:ok, storage} = ETS.insert(storage, :parent, ["alice", "bob"])
      {:ok, storage} = ETS.insert(storage, :parent, ["bob", "carol"])

      # Rules:
      # ancestor(X, Y) :- parent(X, Y)
      # ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z)
      rules = [
        Rule.new({:ancestor, [:X, :Y]}, [{:parent, [:X, :Y]}]),
        Rule.new({:ancestor, [:X, :Z]}, [{:parent, [:X, :Y]}, {:ancestor, [:Y, :Z]}])
      ]

      {:ok, derived, _storage} = Evaluator.evaluate(rules, storage, ETS)

      assert {:ancestor, ["alice", "bob"]} in derived
      assert {:ancestor, ["bob", "carol"]} in derived
      assert {:ancestor, ["alice", "carol"]} in derived
    end
  end

  describe "evaluate/3 with negation" do
    test "handles stratified negation", %{storage: storage} do
      # Facts
      {:ok, storage} = ETS.insert(storage, :user, ["alice"])
      {:ok, storage} = ETS.insert(storage, :user, ["bob"])
      {:ok, storage} = ETS.insert(storage, :banned, ["bob"])

      # Rule: active(X) :- user(X), not banned(X)
      rules = [
        Rule.new({:active, [:X]}, [{:user, [:X]}], negations: [{:banned, [:X]}])
      ]

      {:ok, derived, _storage} = Evaluator.evaluate(rules, storage, ETS)

      assert {:active, ["alice"]} in derived
      refute {:active, ["bob"]} in derived
    end
  end

  describe "evaluate/3 with joins" do
    test "handles multi-predicate joins", %{storage: storage} do
      # Facts
      {:ok, storage} = ETS.insert(storage, :user, ["alice", :admin])
      {:ok, storage} = ETS.insert(storage, :user, ["bob", :viewer])
      {:ok, storage} = ETS.insert(storage, :permission, [:admin, "doc1"])
      {:ok, storage} = ETS.insert(storage, :permission, [:admin, "doc2"])

      # Rule: can_access(U, D) :- user(U, R), permission(R, D)
      rules = [
        Rule.new({:can_access, [:U, :D]}, [{:user, [:U, :R]}, {:permission, [:R, :D]}])
      ]

      {:ok, derived, _storage} = Evaluator.evaluate(rules, storage, ETS)

      assert {:can_access, ["alice", "doc1"]} in derived
      assert {:can_access, ["alice", "doc2"]} in derived
      refute Enum.any?(derived, fn {pred, [user, _]} -> pred == :can_access and user == "bob" end)
    end
  end
end

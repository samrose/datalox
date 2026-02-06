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

  describe "evaluate/3 with join ordering" do
    test "produces correct results regardless of body goal order", %{storage: storage} do
      # Insert a large permission set and small user set
      storage =
        Enum.reduce(1..50, storage, fn i, st ->
          {:ok, st} = ETS.insert(st, :permission, [:admin, "doc_#{i}"])
          st
        end)

      {:ok, storage} = ETS.insert(storage, :user, ["alice", :admin])

      # Rule with large relation first (suboptimal order)
      rules = [
        Rule.new({:can_access, [:U, :D]}, [{:permission, [:R, :D]}, {:user, [:U, :R]}])
      ]

      {:ok, derived, _storage} = Evaluator.evaluate(rules, storage, ETS)

      # Should still derive all 50 can_access facts
      access_facts = Enum.filter(derived, fn {pred, _} -> pred == :can_access end)
      assert length(access_facts) == 50
      assert {:can_access, ["alice", "doc_1"]} in derived
    end
  end

  describe "evaluate/3 with aggregation" do
    test "computes count aggregation", %{storage: storage} do
      {:ok, storage} = ETS.insert(storage, :employee, ["alice", "engineering"])
      {:ok, storage} = ETS.insert(storage, :employee, ["bob", "engineering"])
      {:ok, storage} = ETS.insert(storage, :employee, ["carol", "sales"])

      rules = [
        Rule.new(
          {:dept_count, [:Dept, :N]},
          [{:employee, [:_, :Dept]}],
          aggregations: [{:count, :N, :_, [:Dept]}]
        )
      ]

      {:ok, derived, _storage} = Evaluator.evaluate(rules, storage, ETS)

      assert {:dept_count, ["engineering", 2]} in derived
      assert {:dept_count, ["sales", 1]} in derived
    end

    test "computes sum aggregation", %{storage: storage} do
      {:ok, storage} = ETS.insert(storage, :sale, ["alice", 100])
      {:ok, storage} = ETS.insert(storage, :sale, ["alice", 200])
      {:ok, storage} = ETS.insert(storage, :sale, ["bob", 50])

      rules = [
        Rule.new(
          {:total_sales, [:Person, :Total]},
          [{:sale, [:Person, :Amount]}],
          aggregations: [{:sum, :Total, :Amount, [:Person]}]
        )
      ]

      {:ok, derived, _storage} = Evaluator.evaluate(rules, storage, ETS)

      assert {:total_sales, ["alice", 300]} in derived
      assert {:total_sales, ["bob", 50]} in derived
    end

    test "computes min aggregation", %{storage: storage} do
      {:ok, storage} = ETS.insert(storage, :score, ["alice", 90])
      {:ok, storage} = ETS.insert(storage, :score, ["alice", 70])
      {:ok, storage} = ETS.insert(storage, :score, ["bob", 85])

      rules = [
        Rule.new(
          {:min_score, [:Person, :Min]},
          [{:score, [:Person, :Val]}],
          aggregations: [{:min, :Min, :Val, [:Person]}]
        )
      ]

      {:ok, derived, _storage} = Evaluator.evaluate(rules, storage, ETS)

      assert {:min_score, ["alice", 70]} in derived
      assert {:min_score, ["bob", 85]} in derived
    end
  end

  describe "evaluate/3 with parallel evaluation" do
    test "correctly evaluates independent rules in same stratum", %{storage: storage} do
      # Two independent rules that don't depend on each other
      {:ok, storage} = ETS.insert(storage, :user, ["alice"])
      {:ok, storage} = ETS.insert(storage, :user, ["bob"])
      {:ok, storage} = ETS.insert(storage, :item, ["widget"])
      {:ok, storage} = ETS.insert(storage, :item, ["gadget"])

      # These rules are independent (different head predicates, no shared derived predicates)
      rules = [
        Rule.new({:active_user, [:X]}, [{:user, [:X]}]),
        Rule.new({:listed_item, [:I]}, [{:item, [:I]}])
      ]

      {:ok, derived, _storage} = Evaluator.evaluate(rules, storage, ETS)

      assert {:active_user, ["alice"]} in derived
      assert {:active_user, ["bob"]} in derived
      assert {:listed_item, ["widget"]} in derived
      assert {:listed_item, ["gadget"]} in derived
    end

    test "correctly evaluates dependent rules across strata", %{storage: storage} do
      {:ok, storage} = ETS.insert(storage, :user, ["alice"])
      {:ok, storage} = ETS.insert(storage, :user, ["bob"])
      {:ok, storage} = ETS.insert(storage, :banned, ["bob"])

      # Stratum 0: banned is base fact
      # Stratum 1: active depends on negation of banned
      # Stratum 2: trusted depends on active
      rules = [
        Rule.new({:active, [:X]}, [{:user, [:X]}], negations: [{:banned, [:X]}]),
        Rule.new({:trusted, [:X]}, [{:active, [:X]}])
      ]

      {:ok, derived, _storage} = Evaluator.evaluate(rules, storage, ETS)

      assert {:active, ["alice"]} in derived
      refute {:active, ["bob"]} in derived
      assert {:trusted, ["alice"]} in derived
      refute {:trusted, ["bob"]} in derived
    end
  end
end

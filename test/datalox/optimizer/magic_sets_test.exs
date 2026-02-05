defmodule Datalox.Optimizer.MagicSetsTest do
  use ExUnit.Case, async: true

  alias Datalox.Optimizer.MagicSets
  alias Datalox.Rule

  describe "transform/2" do
    test "creates magic seed fact from bound query arguments" do
      # Query: ancestor("alice", :_)
      query = {:ancestor, ["alice", :_]}

      rules = [
        Rule.new({:ancestor, [:X, :Y]}, [{:parent, [:X, :Y]}])
      ]

      {magic_facts, _transformed_rules} = MagicSets.transform(rules, query)

      # Should create magic_ancestor("alice")
      assert {:magic_ancestor, ["alice"]} in magic_facts
    end

    test "transforms rules to include magic predicate" do
      query = {:ancestor, ["alice", :_]}

      rules = [
        Rule.new({:ancestor, [:X, :Y]}, [{:parent, [:X, :Y]}])
      ]

      {_magic_facts, transformed_rules} = MagicSets.transform(rules, query)

      # The transformed rule should have magic_ancestor in body
      transformed = hd(transformed_rules)
      body_preds = Enum.map(transformed.body, fn {pred, _} -> pred end)

      assert :magic_ancestor in body_preds
    end

    test "generates magic propagation rules for recursive predicates" do
      query = {:ancestor, ["alice", :_]}

      # Recursive rule: ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z)
      rules = [
        Rule.new({:ancestor, [:X, :Y]}, [{:parent, [:X, :Y]}]),
        Rule.new({:ancestor, [:X, :Z]}, [{:parent, [:X, :Y]}, {:ancestor, [:Y, :Z]}])
      ]

      {_magic_facts, transformed_rules} = MagicSets.transform(rules, query)

      # Should have a magic propagation rule:
      # magic_ancestor(Y) :- magic_ancestor(X), parent(X, Y)
      magic_rules =
        Enum.filter(transformed_rules, fn rule ->
          {pred, _} = rule.head
          pred == :magic_ancestor
        end)

      assert magic_rules != []
    end
  end

  describe "magic_predicate/1" do
    test "creates magic predicate name" do
      assert MagicSets.magic_predicate(:ancestor) == :magic_ancestor
      assert MagicSets.magic_predicate(:can_access) == :magic_can_access
    end
  end
end

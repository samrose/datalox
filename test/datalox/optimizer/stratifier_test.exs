defmodule Datalox.Optimizer.StratifierTest do
  use ExUnit.Case, async: true

  alias Datalox.Optimizer.Stratifier
  alias Datalox.Rule

  describe "stratify/1" do
    test "stratifies rules without negation" do
      rules = [
        Rule.new({:ancestor, [:X, :Z]}, [{:parent, [:X, :Z]}]),
        Rule.new({:ancestor, [:X, :Z]}, [{:parent, [:X, :Y]}, {:ancestor, [:Y, :Z]}])
      ]

      assert {:ok, strata} = Stratifier.stratify(rules)
      assert length(strata) == 1
      assert length(hd(strata)) == 2
    end

    test "stratifies rules with negation" do
      rules = [
        Rule.new({:active, [:User]}, [{:user, [:User]}], negations: [{:banned, [:User]}]),
        Rule.new({:banned, [:User]}, [{:violation, [:User, :_]}])
      ]

      assert {:ok, strata} = Stratifier.stratify(rules)
      # banned must be in earlier stratum than active
      assert length(strata) == 2
    end

    test "detects circular negation" do
      rules = [
        Rule.new({:a, [:X]}, [{:b, [:X]}], negations: [{:c, [:X]}]),
        Rule.new({:c, [:X]}, [{:d, [:X]}], negations: [{:a, [:X]}])
      ]

      assert {:error, {:circular_negation, cycle}} = Stratifier.stratify(rules)
      assert :a in cycle
      assert :c in cycle
    end

    test "handles base predicates" do
      rules = [
        Rule.new({:can_access, [:U, :R]}, [
          {:user, [:U, :Role]},
          {:permission, [:Role, :R]}
        ])
      ]

      assert {:ok, strata} = Stratifier.stratify(rules)
      assert length(strata) == 1
    end
  end

  describe "dependency_graph/1" do
    test "builds predicate dependency graph" do
      rules = [
        Rule.new({:ancestor, [:X, :Z]}, [{:parent, [:X, :Y]}, {:ancestor, [:Y, :Z]}]),
        Rule.new({:can_reach, [:X, :Y]}, [{:ancestor, [:X, :Y]}])
      ]

      graph = Stratifier.dependency_graph(rules)

      assert :parent in graph[:ancestor].positive
      assert :ancestor in graph[:ancestor].positive
      assert :ancestor in graph[:can_reach].positive
    end

    test "tracks negative dependencies" do
      rules = [
        Rule.new({:active, [:U]}, [{:user, [:U]}], negations: [{:banned, [:U]}])
      ]

      graph = Stratifier.dependency_graph(rules)

      assert :user in graph[:active].positive
      assert :banned in graph[:active].negative
    end
  end
end

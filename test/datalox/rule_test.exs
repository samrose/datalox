defmodule Datalox.RuleTest do
  use ExUnit.Case, async: true

  alias Datalox.Rule

  describe "new/2" do
    test "creates a simple rule" do
      rule =
        Rule.new(
          {:ancestor, [:X, :Z]},
          [{:parent, [:X, :Z]}]
        )

      assert rule.head == {:ancestor, [:X, :Z]}
      assert rule.body == [{:parent, [:X, :Z]}]
      assert rule.negations == []
      assert rule.aggregations == []
    end

    test "creates a rule with negation" do
      rule =
        Rule.new(
          {:inactive, [:User]},
          [{:user, [:User, :_, :_]}],
          negations: [{:login, [:User, :_]}]
        )

      assert rule.negations == [{:login, [:User, :_]}]
    end

    test "creates a rule with aggregation" do
      rule =
        Rule.new(
          {:high_spender, [:User]},
          [{:user, [:User, :_, :_]}],
          aggregations: [
            {:sum, :amount, {:purchase, [:User, :_, :amount]}, {:>, 1000}}
          ]
        )

      assert length(rule.aggregations) == 1
    end
  end

  describe "variables/1" do
    test "extracts all variables from rule" do
      rule =
        Rule.new(
          {:ancestor, [:X, :Z]},
          [{:parent, [:X, :Y]}, {:ancestor, [:Y, :Z]}]
        )

      vars = Rule.variables(rule)
      assert MapSet.equal?(vars, MapSet.new([:X, :Y, :Z]))
    end

    test "ignores underscore wildcards" do
      rule =
        Rule.new(
          {:active, [:User]},
          [{:user, [:User, :_, :_]}]
        )

      vars = Rule.variables(rule)
      assert MapSet.equal?(vars, MapSet.new([:User]))
    end
  end

  describe "head_variables/1" do
    test "returns variables in rule head" do
      rule =
        Rule.new(
          {:ancestor, [:X, :Z]},
          [{:parent, [:X, :Y]}, {:ancestor, [:Y, :Z]}]
        )

      vars = Rule.head_variables(rule)
      assert MapSet.equal?(vars, MapSet.new([:X, :Z]))
    end
  end

  describe "depends_on/1" do
    test "returns predicates the rule depends on" do
      rule =
        Rule.new(
          {:can_access, [:User, :Resource]},
          [{:user, [:User, :Role, :Dept]}, {:resource, [:Resource, :Dept]}],
          negations: [{:banned, [:User]}]
        )

      deps = Rule.depends_on(rule)
      assert MapSet.equal?(deps, MapSet.new([:user, :resource, :banned]))
    end
  end

  describe "recursive?/1" do
    test "detects recursive rules" do
      rule =
        Rule.new(
          {:ancestor, [:X, :Z]},
          [{:parent, [:X, :Y]}, {:ancestor, [:Y, :Z]}]
        )

      assert Rule.recursive?(rule)
    end

    test "detects non-recursive rules" do
      rule =
        Rule.new(
          {:can_access, [:User, :Resource]},
          [{:user, [:User, :Role]}, {:permission, [:Role, :Resource]}]
        )

      refute Rule.recursive?(rule)
    end
  end
end

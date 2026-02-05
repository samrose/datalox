defmodule Datalox.IncrementalTest do
  use ExUnit.Case, async: true

  alias Datalox.Incremental
  alias Datalox.Rule

  describe "compute_delta/3" do
    test "computes new derivations from inserted fact" do
      # Rule: ancestor(X, Y) :- parent(X, Y)
      rule = Rule.new({:ancestor, [:X, :Y]}, [{:parent, [:X, :Y]}])

      # Delta: new parent fact
      delta = {:parent, ["alice", "bob"]}

      # Existing facts (empty)
      facts = %{}

      new_facts = Incremental.compute_delta(rule, delta, facts)

      assert {:ancestor, ["alice", "bob"]} in new_facts
    end

    test "computes transitive derivations" do
      # Rule: ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z)
      rule = Rule.new({:ancestor, [:X, :Z]}, [{:parent, [:X, :Y]}, {:ancestor, [:Y, :Z]}])

      # Delta: new parent fact
      delta = {:parent, ["alice", "bob"]}

      # Existing facts
      facts = %{
        ancestor: [["bob", "carol"]]
      }

      new_facts = Incremental.compute_delta(rule, delta, facts)

      assert {:ancestor, ["alice", "carol"]} in new_facts
    end
  end

  describe "affected_rules/2" do
    test "finds rules affected by a predicate change" do
      rules = [
        Rule.new({:ancestor, [:X, :Y]}, [{:parent, [:X, :Y]}]),
        Rule.new({:can_access, [:U, :R]}, [{:user, [:U, :_]}, {:resource, [:R, :_]}])
      ]

      affected = Incremental.affected_rules(rules, :parent)

      assert length(affected) == 1
      assert hd(affected).head == {:ancestor, [:X, :Y]}
    end
  end
end

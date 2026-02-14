defmodule Datalox.ExplainTest do
  use ExUnit.Case, async: true

  describe "explain/2" do
    setup do
      name = :"test_#{:erlang.unique_integer()}"
      {:ok, db} = Datalox.new(name: name)

      # Set up facts
      Datalox.assert(db, {:parent, ["alice", "bob"]})
      Datalox.assert(db, {:parent, ["bob", "carol"]})

      # Set up rules
      rules = [
        Datalox.Rule.new({:ancestor, [:X, :Y]}, [{:parent, [:X, :Y]}]),
        Datalox.Rule.new({:ancestor, [:X, :Z]}, [{:parent, [:X, :Y]}, {:ancestor, [:Y, :Z]}])
      ]

      Datalox.Database.load_rules(db, rules)

      on_exit(fn -> catch_exit(Datalox.stop(db)) end)
      {:ok, db: db}
    end

    test "explains a base fact", %{db: db} do
      explanation = Datalox.explain(db, {:parent, ["alice", "bob"]})

      assert explanation.fact == {:parent, ["alice", "bob"]}
      assert explanation.derivation == :base
    end

    test "explains a derived fact", %{db: db} do
      explanation = Datalox.explain(db, {:ancestor, ["alice", "bob"]})

      assert explanation.fact == {:ancestor, ["alice", "bob"]}
      assert explanation.rule == :ancestor
      assert is_list(explanation.derivation)
      assert [_ | _] = explanation.derivation

      # The body goal is parent("alice", "bob") which is a base fact
      [body_fact | _] = explanation.derivation
      assert body_fact.fact == {:parent, ["alice", "bob"]}
      assert body_fact.derivation == :base
    end

    test "explains a transitively derived fact with sub-derivations", %{db: db} do
      explanation = Datalox.explain(db, {:ancestor, ["alice", "carol"]})

      assert explanation.fact == {:ancestor, ["alice", "carol"]}
      assert explanation.rule == :ancestor
      assert is_list(explanation.derivation)

      # Should have sub-derivations (parent + ancestor body goals)
      derivation_facts = Enum.map(explanation.derivation, & &1.fact)

      # One body goal must be a parent fact, the other an ancestor
      assert Enum.any?(derivation_facts, fn {pred, _} -> pred == :parent end)
    end

    test "returns nil for non-existent fact", %{db: db} do
      assert Datalox.explain(db, {:nonexistent, ["x"]}) == nil
    end
  end
end

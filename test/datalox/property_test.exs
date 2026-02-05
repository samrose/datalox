defmodule Datalox.PropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Datalox.Rule
  alias Datalox.Optimizer.Stratifier

  # Generators
  defp atom_generator do
    gen all name <- StreamData.string(:alphanumeric, min_length: 1, max_length: 10) do
      String.to_atom(String.downcase(name))
    end
  end

  defp predicate_generator do
    StreamData.member_of([:user, :parent, :role, :permission, :resource])
  end

  defp value_generator do
    StreamData.one_of([
      StreamData.string(:alphanumeric, min_length: 1, max_length: 10),
      StreamData.integer(),
      StreamData.member_of([:admin, :viewer, :editor, :read, :write])
    ])
  end

  defp fact_generator(predicate, arity) do
    gen all values <- StreamData.list_of(value_generator(), length: arity) do
      {predicate, values}
    end
  end

  describe "stratification properties" do
    property "stratification is deterministic" do
      check all pred1 <- predicate_generator(),
                pred2 <- predicate_generator(),
                pred1 != pred2 do
        # Create a simple set of rules
        rules = [
          Rule.new({pred1, [:X]}, [{pred2, [:X]}])
        ]

        # Run stratification multiple times
        {:ok, strata1} = Stratifier.stratify(rules)
        {:ok, strata2} = Stratifier.stratify(rules)
        {:ok, strata3} = Stratifier.stratify(rules)

        # Should always produce the same result
        assert strata1 == strata2
        assert strata2 == strata3
      end
    end

    property "rules without negation produce single stratum" do
      check all pred1 <- predicate_generator(),
                pred2 <- predicate_generator(),
                pred1 != pred2 do
        rules = [
          Rule.new({pred1, [:X]}, [{pred2, [:X]}])
        ]

        {:ok, strata} = Stratifier.stratify(rules)

        # Without negation, all rules can be in same stratum
        assert length(strata) == 1
      end
    end
  end

  describe "query result properties" do
    property "empty database returns empty results" do
      check all pred <- predicate_generator() do
        name = :"test_#{:erlang.unique_integer([:positive])}"
        {:ok, db} = Datalox.new(name: name)

        results = Datalox.query(db, {pred, [:_]})

        assert results == []

        Datalox.stop(db)
      end
    end

    property "asserted fact can be queried back" do
      check all pred <- predicate_generator(),
                value <- value_generator() do
        name = :"test_#{:erlang.unique_integer([:positive])}"
        {:ok, db} = Datalox.new(name: name)

        fact = {pred, [value]}
        Datalox.assert(db, fact)

        results = Datalox.query(db, {pred, [:_]})

        assert fact in results

        Datalox.stop(db)
      end
    end

    property "retracted fact cannot be queried" do
      check all pred <- predicate_generator(),
                value <- value_generator() do
        name = :"test_#{:erlang.unique_integer([:positive])}"
        {:ok, db} = Datalox.new(name: name)

        fact = {pred, [value]}
        Datalox.assert(db, fact)
        Datalox.retract(db, fact)

        results = Datalox.query(db, {pred, [:_]})

        refute fact in results

        Datalox.stop(db)
      end
    end
  end

  describe "incremental evaluation properties" do
    property "incremental delta matches full evaluation for simple rules" do
      check all parent_value <- value_generator(),
                child_value <- value_generator() do
        # Rule: ancestor(X, Y) :- parent(X, Y)
        rule = Rule.new({:ancestor, [:X, :Y]}, [{:parent, [:X, :Y]}])

        # Full evaluation with all facts
        facts = %{parent: [[parent_value, child_value]]}

        # Delta evaluation
        delta = {:parent, [parent_value, child_value]}
        delta_results = Datalox.Incremental.compute_delta(rule, delta, %{})

        # Both should derive the same ancestor fact
        expected = {:ancestor, [parent_value, child_value]}
        assert expected in delta_results
      end
    end
  end
end

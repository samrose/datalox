defmodule Datalox.ErrorsTest do
  use ExUnit.Case, async: true

  alias Datalox.Errors.{
    ArityError,
    SafetyError,
    StratificationError,
    TypeError,
    UndefinedPredicateError
  }

  describe "StratificationError" do
    test "formats message with cycle path" do
      error = %StratificationError{
        cycle: [:bad, :bar, :bad],
        location: "my_rules.ex:12"
      }

      message = Exception.message(error)
      assert message =~ "Circular negation detected"
      assert message =~ "bad -> bar -> bad"
      assert message =~ "my_rules.ex:12"
    end
  end

  describe "UndefinedPredicateError" do
    test "suggests similar predicates" do
      error = %UndefinedPredicateError{
        predicate: :usr,
        suggestions: [:user],
        location: "my_rules.ex:8"
      }

      message = Exception.message(error)
      assert message =~ "Unknown predicate 'usr'"
      assert message =~ "Did you mean: user"
    end
  end

  describe "ArityError" do
    test "shows expected vs actual arity" do
      error = %ArityError{
        predicate: :user,
        expected: 3,
        actual: 2,
        location: "my_rules.ex:15"
      }

      message = Exception.message(error)
      assert message =~ "expects 3 arguments, got 2"
    end
  end

  describe "SafetyError" do
    test "identifies unsafe variable" do
      error = %SafetyError{
        variable: :X,
        context: "not banned(X)",
        location: "my_rules.ex:20"
      }

      message = Exception.message(error)
      assert message =~ "Variable 'X' is unsafe"
      assert message =~ "not banned(X)"
    end
  end

  describe "TypeError" do
    test "shows type mismatch in aggregation" do
      error = %TypeError{
        operation: :sum,
        expected: :numeric,
        actual: "not_a_number",
        context: "purchase/3"
      }

      message = Exception.message(error)
      assert message =~ "Cannot sum non-numeric value"
    end
  end
end

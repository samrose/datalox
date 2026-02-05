defmodule Datalox.Errors do
  @moduledoc """
  Custom error types for Datalox with helpful diagnostic messages.
  """

  defmodule StratificationError do
    @moduledoc """
    Raised when rules contain circular negation that cannot be stratified.
    """
    defexception [:cycle, :location]

    @impl true
    def message(%{cycle: cycle, location: location}) do
      cycle_str = Enum.join(cycle, " -> ")

      """
      Circular negation detected

         #{cycle_str}

         Rules with negation cannot form cycles. Consider:
         - Restructuring rules to break the cycle
         - Using positive conditions instead

         at #{location}
      """
    end
  end

  defmodule UndefinedPredicateError do
    @moduledoc """
    Raised when a rule references an undefined predicate.
    """
    defexception [:predicate, :suggestions, :location]

    @impl true
    def message(%{predicate: predicate, suggestions: suggestions, location: location}) do
      suggestion_str =
        case suggestions do
          [] -> ""
          [s] -> "\n   Did you mean: #{s}?"
          list -> "\n   Did you mean one of: #{Enum.join(list, ", ")}?"
        end

      """
      Unknown predicate '#{predicate}' in rule#{suggestion_str}

         at #{location}
      """
    end
  end

  defmodule ArityError do
    @moduledoc """
    Raised when a predicate is used with the wrong number of arguments.
    """
    defexception [:predicate, :expected, :actual, :location]

    @impl true
    def message(%{predicate: predicate, expected: expected, actual: actual, location: location}) do
      """
      Predicate '#{predicate}' expects #{expected} arguments, got #{actual}

         at #{location}
      """
    end
  end

  defmodule SafetyError do
    @moduledoc """
    Raised when a variable only appears in negated goals.
    """
    defexception [:variable, :context, :location]

    @impl true
    def message(%{variable: variable, context: context, location: location}) do
      """
      Variable '#{variable}' is unsafe

         #{variable} only appears in negated goal: #{context}
         Variables must appear in at least one positive goal.

         at #{location}
      """
    end
  end

  defmodule TypeError do
    @moduledoc """
    Raised when a type mismatch occurs during evaluation.
    """
    defexception [:operation, :expected, :actual, :context]

    @impl true
    def message(%{operation: operation, expected: _expected, actual: actual, context: context}) do
      """
      Cannot #{operation} non-numeric value

         Got: #{inspect(actual)} in #{context}
      """
    end
  end
end

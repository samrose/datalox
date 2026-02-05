defmodule Datalox.DSL do
  @moduledoc """
  Macro-based DSL for defining Datalog facts and rules.

  ## Usage

      defmodule MyRules do
        use Datalox.DSL

        deffact user("alice", :admin)
        deffact user("bob", :viewer)

        defrule can_access(user, resource) do
          user(user, role) and permission(role, resource)
        end

        defrule active(user) do
          user(user) and not banned(user)
        end
      end

  """

  defmacro __using__(_opts) do
    quote do
      import Datalox.DSL
      Module.register_attribute(__MODULE__, :datalox_facts, accumulate: true)
      Module.register_attribute(__MODULE__, :datalox_rules, accumulate: true)

      @before_compile Datalox.DSL
    end
  end

  defmacro __before_compile__(_env) do
    quote do
      def __datalox_facts__ do
        @datalox_facts |> Enum.reverse()
      end

      def __datalox_rules__ do
        @datalox_rules |> Enum.reverse()
      end
    end
  end

  @doc """
  Defines a fact.

  ## Example

      deffact user("alice", :admin)

  """
  defmacro deffact(fact) do
    {predicate, args} = parse_goal(fact)
    values = Enum.map(args, &Macro.escape/1)

    quote do
      @datalox_facts {unquote(predicate), unquote(values)}
    end
  end

  @doc """
  Defines a rule.

  ## Example

      defrule ancestor(x, y) do
        parent(x, y)
      end

  """
  defmacro defrule(head, do: body) do
    {head_pred, head_args} = parse_goal(head)
    head_vars = Enum.map(head_args, &var_to_atom/1)

    {body_goals, negations} = parse_body(body)

    quote do
      @datalox_rules %Datalox.Rule{
        head: {unquote(head_pred), unquote(head_vars)},
        body: unquote(Macro.escape(body_goals)),
        negations: unquote(Macro.escape(negations)),
        aggregations: [],
        guards: []
      }
    end
  end

  # Parse a goal like `user("alice", :admin)` into {:user, ["alice", :admin]}
  defp parse_goal({predicate, _, args}) when is_atom(predicate) do
    {predicate, args || []}
  end

  # Convert variable AST to uppercase atom (Datalog convention)
  defp var_to_atom({name, _, _}) when is_atom(name) do
    name
    |> Atom.to_string()
    |> String.upcase()
    |> String.to_atom()
  end

  defp var_to_atom(literal), do: literal

  # Parse body into goals and negations
  defp parse_body({:and, _, [left, right]}) do
    {left_goals, left_negs} = parse_body(left)
    {right_goals, right_negs} = parse_body(right)
    {left_goals ++ right_goals, left_negs ++ right_negs}
  end

  defp parse_body({:not, _, [goal]}) do
    {predicate, args} = parse_goal(goal)
    vars = Enum.map(args, &var_to_atom/1)
    {[], [{predicate, vars}]}
  end

  defp parse_body(goal) do
    {predicate, args} = parse_goal(goal)
    vars = Enum.map(args, &var_to_atom/1)
    {[{predicate, vars}], []}
  end
end

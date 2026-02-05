defmodule Datalox.Optimizer.MagicSets do
  @moduledoc """
  Magic Sets query optimization for Datalox.

  Transforms rules to be goal-directed, deriving only facts
  relevant to the specific query pattern.
  """

  alias Datalox.Rule

  @doc """
  Creates a magic predicate name from a regular predicate.
  """
  @spec magic_predicate(atom()) :: atom()
  def magic_predicate(predicate) do
    :"magic_#{predicate}"
  end

  @doc """
  Transforms rules using Magic Sets optimization for the given query.

  Returns {magic_seed_facts, transformed_rules}.
  """
  @spec transform([Rule.t()], {atom(), list()}) :: {[{atom(), list()}], [Rule.t()]}
  def transform(rules, {query_pred, query_args}) do
    magic_pred = magic_predicate(query_pred)

    # Create magic seed fact from bound arguments
    bound_args = extract_bound_args(query_args)
    magic_seed = {magic_pred, bound_args}

    # Find rules that derive the query predicate
    relevant_rules =
      Enum.filter(rules, fn rule ->
        {head_pred, _} = rule.head
        head_pred == query_pred
      end)

    # Transform each rule and generate magic propagation rules
    {transformed, propagation_rules} =
      relevant_rules
      |> Enum.map(&transform_rule(&1, query_pred, bound_args))
      |> Enum.unzip()

    all_rules = List.flatten(propagation_rules) ++ transformed

    {[magic_seed], all_rules}
  end

  # Extract bound (non-wildcard) arguments from query
  defp extract_bound_args(args) do
    Enum.filter(args, &(&1 != :_))
  end

  # Transform a single rule to include magic predicate
  defp transform_rule(rule, query_pred, bound_args) do
    {head_pred, head_vars} = rule.head
    magic_pred = magic_predicate(head_pred)

    # Find which head variables are bound by the query
    bound_positions =
      bound_args
      |> Enum.with_index()
      |> Enum.map(fn {_, idx} -> idx end)

    bound_head_vars =
      head_vars
      |> Enum.with_index()
      |> Enum.filter(fn {_, idx} -> idx in bound_positions end)
      |> Enum.map(fn {var, _} -> var end)

    # Add magic predicate to body
    magic_goal = {magic_pred, bound_head_vars}
    new_body = [magic_goal | rule.body]

    transformed_rule = %Rule{rule | body: new_body}

    # Generate magic propagation rules for recursive predicates
    propagation_rules = generate_propagation_rules(rule, query_pred, bound_head_vars)

    {transformed_rule, propagation_rules}
  end

  # Generate magic propagation rules for recursive predicates
  defp generate_propagation_rules(rule, query_pred, bound_vars) do
    magic_pred = magic_predicate(query_pred)

    # Check if rule has recursive call to query_pred in body
    recursive_goals = Enum.filter(rule.body, fn {pred, _} -> pred == query_pred end)

    Enum.map(recursive_goals, fn {_pred, goal_vars} ->
      # Find which variable from bound_vars flows into the recursive call
      # For ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z)
      # We need: magic_ancestor(Y) :- magic_ancestor(X), parent(X, Y)

      # Get the first bound variable from the recursive call
      target_var = hd(goal_vars)

      # Find the goal that provides the binding for target_var
      binding_goals =
        Enum.filter(rule.body, fn {pred, vars} ->
          pred != query_pred and target_var in vars
        end)

      # Create magic propagation rule
      magic_body = [{magic_pred, bound_vars}] ++ binding_goals

      Rule.new({magic_pred, [target_var]}, magic_body)
    end)
  end
end

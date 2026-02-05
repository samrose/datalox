defmodule Datalox.Incremental do
  @moduledoc """
  Incremental maintenance for Datalox.

  Efficiently updates derived facts when base facts change,
  using semi-naive delta evaluation.
  """

  alias Datalox.Rule

  @doc """
  Finds rules that are affected by a change to the given predicate.
  """
  @spec affected_rules([Rule.t()], atom()) :: [Rule.t()]
  def affected_rules(rules, predicate) do
    Enum.filter(rules, fn rule ->
      predicate in Rule.depends_on(rule)
    end)
  end

  @doc """
  Computes new derivations from a delta (newly inserted fact).

  Uses semi-naive evaluation: only consider derivations that
  use the new fact.
  """
  @spec compute_delta(Rule.t(), {atom(), list()}, map()) :: [{atom(), list()}]
  def compute_delta(rule, {delta_pred, delta_args}, existing_facts) do
    {head_pred, head_vars} = rule.head

    # Find which body goals match the delta predicate
    rule.body
    |> Enum.with_index()
    |> Enum.filter(fn {{pred, _}, _} -> pred == delta_pred end)
    |> Enum.flat_map(fn {{_pred, goal_vars}, delta_index} ->
      # Create initial binding from delta
      initial_binding = create_binding(goal_vars, delta_args)

      # Evaluate remaining body goals with existing facts
      other_goals = List.delete_at(rule.body, delta_index)
      bindings = evaluate_goals(other_goals, [initial_binding], existing_facts)

      # Project to head
      Enum.map(bindings, fn binding ->
        args = Enum.map(head_vars, &Map.get(binding, &1))
        {head_pred, args}
      end)
    end)
    |> Enum.uniq()
  end

  @doc """
  Computes facts to retract when a base fact is deleted.

  Uses truth maintenance: re-derive facts to see if they
  can still be derived without the deleted fact.
  """
  @spec compute_retractions(Rule.t(), {atom(), list()}, map()) :: [{atom(), list()}]
  def compute_retractions(rule, deleted_fact, existing_facts) do
    {head_pred, _} = rule.head

    # Find all derived facts from this rule
    derived = Map.get(existing_facts, head_pred, [])

    # For each derived fact, check if it can still be derived
    # without the deleted fact
    facts_without_deleted = remove_fact(existing_facts, deleted_fact)

    Enum.filter(derived, fn args ->
      # Try to re-derive this fact
      not can_derive?(rule, {head_pred, args}, facts_without_deleted)
    end)
    |> Enum.map(fn args -> {head_pred, args} end)
  end

  # Create variable binding from goal pattern and actual values
  defp create_binding(vars, values) do
    Enum.zip(vars, values)
    |> Enum.filter(fn {var, _} -> is_atom(var) and var != :_ end)
    |> Map.new()
  end

  # Evaluate body goals against existing facts
  defp evaluate_goals([], bindings, _facts), do: bindings

  defp evaluate_goals([{pred, goal_vars} | rest], bindings, facts) do
    fact_tuples = Map.get(facts, pred, [])

    new_bindings =
      for binding <- bindings,
          tuple <- fact_tuples,
          new_binding = unify(goal_vars, tuple, binding),
          new_binding != nil do
        new_binding
      end

    evaluate_goals(rest, new_bindings, facts)
  end

  # Unify goal pattern with fact tuple under current binding
  defp unify(vars, values, binding) when length(vars) == length(values) do
    Enum.zip(vars, values)
    |> Enum.reduce_while(binding, fn {var, val}, acc ->
      cond do
        var == :_ -> {:cont, acc}
        is_atom(var) and not Map.has_key?(acc, var) -> {:cont, Map.put(acc, var, val)}
        is_atom(var) and Map.get(acc, var) == val -> {:cont, acc}
        is_atom(var) -> {:halt, nil}
        var == val -> {:cont, acc}
        true -> {:halt, nil}
      end
    end)
  end

  defp unify(_, _, _), do: nil

  defp remove_fact(facts, {pred, args}) do
    Map.update(facts, pred, [], fn tuples ->
      Enum.reject(tuples, &(&1 == args))
    end)
  end

  defp can_derive?(rule, {_pred, target_args}, facts) do
    {_, head_vars} = rule.head

    # Try to find a derivation for the target
    bindings = evaluate_goals(rule.body, [%{}], facts)

    Enum.any?(bindings, fn binding ->
      derived_args = Enum.map(head_vars, &Map.get(binding, &1))
      derived_args == target_args
    end)
  end
end

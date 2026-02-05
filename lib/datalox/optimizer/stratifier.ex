defmodule Datalox.Optimizer.Stratifier do
  @moduledoc """
  Computes stratification for Datalog rules with negation.

  Stratification partitions rules into layers (strata) such that:
  1. If rule R uses predicate P positively, P can be in same or earlier stratum
  2. If rule R uses predicate P negatively, P must be in strictly earlier stratum

  This ensures negation is "safe" - we only negate predicates whose
  complete extension is already computed.
  """

  alias Datalox.Rule

  @type predicate :: atom()
  @type stratum :: [Rule.t()]
  @type dependency :: %{positive: MapSet.t(predicate()), negative: MapSet.t(predicate())}

  @doc """
  Stratifies a list of rules.

  Returns `{:ok, strata}` where strata is a list of rule lists,
  ordered from first to evaluate to last.

  Returns `{:error, {:circular_negation, cycle}}` if rules cannot be stratified.
  """
  @spec stratify([Rule.t()]) :: {:ok, [stratum()]} | {:error, {:circular_negation, [predicate()]}}
  def stratify(rules) do
    graph = dependency_graph(rules)
    predicates = Map.keys(graph)

    case compute_strata(predicates, graph) do
      {:ok, predicate_strata} ->
        rule_strata = assign_rules_to_strata(rules, predicate_strata)
        {:ok, rule_strata}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Builds a dependency graph from rules.

  Returns a map from predicate -> %{positive: MapSet, negative: MapSet}
  """
  @spec dependency_graph([Rule.t()]) :: %{predicate() => dependency()}
  def dependency_graph(rules) do
    rules
    |> Enum.reduce(%{}, fn rule, acc ->
      {head_pred, _} = rule.head
      deps = Rule.depends_on(rule)

      positive_deps =
        rule.body
        |> Enum.map(fn {pred, _} -> pred end)
        |> MapSet.new()

      negative_deps =
        rule.negations
        |> Enum.map(fn {pred, _} -> pred end)
        |> MapSet.new()

      current = Map.get(acc, head_pred, %{positive: MapSet.new(), negative: MapSet.new()})

      updated = %{
        positive: MapSet.union(current.positive, positive_deps),
        negative: MapSet.union(current.negative, negative_deps)
      }

      # Also ensure all referenced predicates are in the graph
      acc = Map.put(acc, head_pred, updated)

      Enum.reduce(deps, acc, fn dep, inner_acc ->
        Map.put_new(inner_acc, dep, %{positive: MapSet.new(), negative: MapSet.new()})
      end)
    end)
  end

  # Compute stratum assignment for each predicate
  defp compute_strata(predicates, graph) do
    initial_strata = Map.new(predicates, fn p -> {p, 0} end)
    iterate_strata(initial_strata, graph, length(predicates) + 1)
  end

  defp iterate_strata(strata, _graph, 0) do
    # If we've iterated more than |predicates| times, we have a cycle
    {:error, {:circular_negation, find_cycle(strata)}}
  end

  defp iterate_strata(strata, graph, remaining) do
    new_strata =
      Enum.reduce(strata, strata, fn {pred, _stratum}, acc ->
        update_predicate_stratum(pred, acc, strata, graph)
      end)

    if new_strata == strata do
      {:ok, strata}
    else
      iterate_strata(new_strata, graph, remaining - 1)
    end
  end

  defp update_predicate_stratum(pred, acc, strata, graph) do
    case Map.get(graph, pred) do
      nil ->
        acc

      %{positive: pos, negative: neg} ->
        new_stratum = compute_predicate_stratum(pos, neg, strata)
        Map.put(acc, pred, max(Map.get(acc, pred, 0), new_stratum))
    end
  end

  defp compute_predicate_stratum(positive_deps, negative_deps, strata) do
    # Stratum must be >= max of positive dependencies
    pos_max =
      positive_deps
      |> Enum.map(&Map.get(strata, &1, 0))
      |> Enum.max(fn -> 0 end)

    # Stratum must be > max of negative dependencies
    neg_max =
      negative_deps
      |> Enum.map(&Map.get(strata, &1, 0))
      |> Enum.max(fn -> -1 end)

    max(pos_max, neg_max + 1)
  end

  # Find predicates involved in the negative cycle
  defp find_cycle(strata) do
    strata
    |> Enum.filter(fn {_pred, stratum} -> stratum > length(Map.keys(strata)) end)
    |> Enum.map(fn {pred, _} -> pred end)
  end

  # Group rules by their head predicate's stratum
  defp assign_rules_to_strata(rules, predicate_strata) do
    max_stratum =
      predicate_strata
      |> Map.values()
      |> Enum.max(fn -> 0 end)

    0..max_stratum
    |> Enum.map(fn stratum ->
      Enum.filter(rules, fn rule ->
        {head_pred, _} = rule.head
        Map.get(predicate_strata, head_pred, 0) == stratum
      end)
    end)
    |> Enum.reject(&Enum.empty?/1)
  end
end

defmodule Datalox.Safety do
  @moduledoc "Range-restriction safety checks for Datalog rules."

  alias Datalox.Rule
  alias Datalox.Unification

  @spec check_all([Rule.t()]) :: :ok | {:error, [String.t()]}
  def check_all(rules) do
    errors = Enum.flat_map(rules, &check_rule/1)
    if errors == [], do: :ok, else: {:error, errors}
  end

  defp check_rule(rule) do
    positive_vars = collect_positive_vars(rule.body)

    # Aggregation target variables are bound by the aggregation itself
    agg_target_vars =
      rule.aggregations
      |> Enum.map(fn {_agg_fn, target, _source, _group_vars} -> target end)
      |> MapSet.new()

    # Function call targets are also bound by the function
    func_target_vars =
      rule.guards
      |> Enum.flat_map(fn
        {:func, target, _name, _args} -> [target]
        _ -> []
      end)
      |> MapSet.new()

    # Assignment guard targets are also bound
    assign_target_vars =
      rule.guards
      |> Enum.flat_map(fn
        {:=, target, _expr} -> [target]
        _ -> []
      end)
      |> MapSet.new()

    head_vars =
      positive_vars
      |> MapSet.union(agg_target_vars)
      |> MapSet.union(func_target_vars)
      |> MapSet.union(assign_target_vars)

    check_head(rule.head, head_vars) ++
      check_negations(rule.negations, positive_vars) ++
      check_aggregations(rule.aggregations, positive_vars) ++
      check_guards(rule.guards, positive_vars)
  end

  defp collect_positive_vars(body) do
    body
    |> Enum.flat_map(fn {_pred, terms} -> terms end)
    |> Enum.filter(&Unification.variable?/1)
    |> Enum.reject(&(&1 == :_))
    |> MapSet.new()
  end

  defp check_head({pred, terms}, positive_vars) do
    var_errors =
      terms
      |> Enum.filter(&Unification.variable?/1)
      |> Enum.reject(&(&1 == :_))
      |> Enum.flat_map(fn var ->
        if MapSet.member?(positive_vars, var),
          do: [],
          else: ["Rule #{pred}: head variable '#{var}' not in any positive body goal"]
      end)

    wildcard_errors =
      if :_ in terms,
        do: ["Rule #{pred}: wildcard :_ not allowed in head"],
        else: []

    var_errors ++ wildcard_errors
  end

  defp check_negations(negations, positive_vars) do
    Enum.flat_map(negations, fn {pred, terms} ->
      terms
      |> filter_real_vars()
      |> check_vars_bound(positive_vars, "Negated goal #{pred}")
    end)
  end

  defp check_aggregations(aggregations, positive_vars) do
    Enum.flat_map(aggregations, fn {agg_fn, _target, _source, group_vars} ->
      group_vars
      |> filter_real_vars()
      |> check_vars_bound(positive_vars, "Aggregation #{agg_fn}")
    end)
  end

  defp check_guards(guards, positive_vars) do
    Enum.flat_map(guards, fn guard ->
      extract_guard_vars(guard)
      |> check_vars_bound(positive_vars, "Guard")
    end)
  end

  defp filter_real_vars(terms) do
    terms |> Enum.filter(&Unification.variable?/1) |> Enum.reject(&(&1 == :_))
  end

  defp check_vars_bound(vars, positive_vars, context) do
    Enum.flat_map(vars, fn var ->
      if MapSet.member?(positive_vars, var),
        do: [],
        else: ["#{context}: variable '#{var}' not in any positive body goal"]
    end)
  end

  defp extract_guard_vars({:func, _target, _name, args}) do
    # Only check that input args are bound; target is an output
    Enum.flat_map(args, &extract_expr_vars/1)
  end

  defp extract_guard_vars({_op, left, right}) do
    extract_expr_vars(left) ++ extract_expr_vars(right)
  end

  defp extract_guard_vars(_), do: []

  defp extract_expr_vars(v) when is_atom(v), do: if(Unification.variable?(v), do: [v], else: [])
  defp extract_expr_vars({_op, a, b}), do: extract_expr_vars(a) ++ extract_expr_vars(b)
  defp extract_expr_vars(_), do: []
end

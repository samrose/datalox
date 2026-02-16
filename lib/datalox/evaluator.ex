defmodule Datalox.Evaluator do
  @moduledoc """
  Semi-naive evaluation for Datalog rules.

  Semi-naive evaluation is an optimization over naive evaluation that
  avoids redundant computation by only considering "new" facts from
  the previous iteration (delta facts).
  """

  alias Datalox.Aggregation
  alias Datalox.Join.Leapfrog
  alias Datalox.Optimizer.{JoinOrder, Stratifier}
  alias Datalox.Rule
  alias Datalox.Unification

  @type fact :: {atom(), list()}
  @type storage :: any()

  @doc """
  Evaluates rules against the current fact base and returns derived facts.

  Returns `{:ok, derived_facts, updated_storage}` on success.
  """
  @spec evaluate([Rule.t()], storage(), module()) ::
          {:ok, [fact()], storage()} | {:error, term()}
  def evaluate(rules, storage, storage_mod), do: evaluate(rules, storage, storage_mod, %{})

  @doc """
  Evaluates rules with optional lattice configuration.

  `lattice_config` maps predicate atoms to `%{key_columns: [int], lattice_column: int, lattice: module}`.
  """
  @spec evaluate([Rule.t()], storage(), module(), map()) ::
          {:ok, [fact()], storage()} | {:error, term()}
  def evaluate(rules, storage, storage_mod, lattice_config) do
    case Stratifier.stratify(rules) do
      {:ok, strata} ->
        evaluate_strata(strata, storage, storage_mod, lattice_config, [])

      {:error, _} = error ->
        error
    end
  end

  # Evaluate each stratum in order
  defp evaluate_strata([], storage, _storage_mod, _lattice_config, acc) do
    {:ok, acc |> Enum.reverse() |> List.flatten(), storage}
  end

  defp evaluate_strata([stratum | rest], storage, storage_mod, lattice_config, acc) do
    {derived, storage} = evaluate_stratum(stratum, storage, storage_mod, lattice_config)
    evaluate_strata(rest, storage, storage_mod, lattice_config, [derived | acc])
  end

  # Evaluate a single stratum using semi-naive iteration
  defp evaluate_stratum(rules, storage, storage_mod, lattice_config) do
    # Initial pass - derive all facts we can
    {initial_facts, storage} = derive_all(rules, storage, storage_mod)

    # Process initial facts through lattice merging (if applicable) and dedup
    {initial_delta_list, initial_derived} =
      process_new_facts(initial_facts, %{}, lattice_config)

    initial_delta = Enum.group_by(initial_delta_list, fn {pred, _} -> pred end)

    iterate_seminaive(rules, storage, storage_mod, lattice_config, initial_delta, initial_derived)
  end

  defp iterate_seminaive(_rules, storage, _storage_mod, _lattice_config, delta, all_derived)
       when map_size(delta) == 0 do
    {Map.keys(all_derived), storage}
  end

  defp iterate_seminaive(rules, storage, storage_mod, lattice_config, delta, all_derived) do
    # Derive new facts using delta
    {new_facts, storage} = derive_with_delta(rules, storage, storage_mod, delta)

    # Process new facts: for lattice predicates, merge; for others, filter duplicates
    {new_delta, all_derived} =
      process_new_facts(new_facts, all_derived, lattice_config)

    if Enum.empty?(new_delta) do
      {Map.keys(all_derived), storage}
    else
      new_delta_map = Enum.group_by(new_delta, fn {pred, _} -> pred end)

      iterate_seminaive(
        rules,
        storage,
        storage_mod,
        lattice_config,
        new_delta_map,
        all_derived
      )
    end
  end

  defp process_new_facts(new_facts, all_derived, lattice_config)
       when map_size(lattice_config) == 0 do
    # No lattices — standard duplicate filtering
    new_delta = Enum.reject(new_facts, fn fact -> Map.has_key?(all_derived, fact) end)

    new_derived =
      Enum.reduce(new_delta, all_derived, fn fact, acc -> Map.put(acc, fact, true) end)

    {new_delta, new_derived}
  end

  defp process_new_facts(new_facts, all_derived, lattice_config) do
    Enum.reduce(new_facts, {[], all_derived}, fn fact, acc ->
      process_single_fact(fact, acc, lattice_config)
    end)
  end

  defp process_single_fact({pred, _tuple} = fact, {delta_acc, derived}, lattice_config) do
    case Map.get(lattice_config, pred) do
      nil ->
        if Map.has_key?(derived, fact),
          do: {delta_acc, derived},
          else: {[fact | delta_acc], Map.put(derived, fact, true)}

      lattice_spec ->
        merge_lattice_fact(fact, lattice_spec, delta_acc, derived)
    end
  end

  defp merge_lattice_fact({pred, tuple}, spec, delta_acc, derived) do
    %{key_columns: key_cols, lattice_column: lat_col, lattice: lattice_mod} = spec
    key = Enum.map(key_cols, fn col -> Enum.at(tuple, col) end)
    new_val = Enum.at(tuple, lat_col)

    existing = find_lattice_existing(derived, pred, key_cols, key)

    case existing do
      nil ->
        {[{pred, tuple} | delta_acc], Map.put(derived, {pred, tuple}, true)}

      {^pred, existing_tuple} ->
        old_val = Enum.at(existing_tuple, lat_col)
        merged_val = lattice_mod.join(old_val, new_val)

        if merged_val == old_val do
          {delta_acc, derived}
        else
          merged_tuple = List.replace_at(tuple, lat_col, merged_val)
          merged_fact = {pred, merged_tuple}
          new_derived = derived |> Map.delete(existing) |> Map.put(merged_fact, true)
          {[merged_fact | delta_acc], new_derived}
        end
    end
  end

  defp find_lattice_existing(derived, pred, key_cols, key) do
    derived
    |> Map.keys()
    |> Enum.find(fn
      {^pred, existing_tuple} ->
        Enum.map(key_cols, fn col -> Enum.at(existing_tuple, col) end) == key

      _ ->
        false
    end)
  end

  # Initial derivation pass — evaluate independent rules in parallel
  defp derive_all(rules, storage, storage_mod) do
    {facts_nested, storage} =
      rules
      |> group_independent_rules()
      |> Enum.reduce({[], storage}, fn group, {acc, st} ->
        {new_facts, st} = evaluate_group(group, st, storage_mod, %{})
        {[new_facts | acc], st}
      end)

    {List.flatten(facts_nested), storage}
  end

  # Derive using delta facts — parallel for independent rules
  defp derive_with_delta(rules, storage, storage_mod, delta) do
    {facts_nested, storage} =
      rules
      |> group_independent_rules()
      |> Enum.reduce({[], storage}, fn group, {acc, st} ->
        {new_facts, st} = evaluate_group(group, st, storage_mod, delta)
        {[new_facts | acc], st}
      end)

    {List.flatten(facts_nested), storage}
  end

  # Evaluate a group of rules — parallel if multiple, sequential if single
  defp evaluate_group([rule], st, storage_mod, delta) do
    {new_facts, _} = evaluate_rule(rule, st, storage_mod, delta)
    st = store_facts(new_facts, st, storage_mod)
    {new_facts, st}
  end

  defp evaluate_group(group, st, storage_mod, delta) do
    results =
      group
      |> Task.async_stream(
        fn rule -> evaluate_rule(rule, st, storage_mod, delta) end,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    all_new_facts = Enum.flat_map(results, fn {new_facts, _} -> new_facts end)
    st = store_facts(all_new_facts, st, storage_mod)
    {all_new_facts, st}
  end

  defp store_facts(facts, storage, storage_mod) do
    Enum.reduce(facts, storage, fn {pred, tuple}, st ->
      {:ok, st} = storage_mod.insert(st, pred, tuple)
      st
    end)
  end

  # Evaluate a single rule — dispatches to full or delta-aware evaluation
  defp evaluate_rule(rule, storage, storage_mod, delta) when map_size(delta) == 0 do
    evaluate_rule_full(rule, storage, storage_mod)
  end

  defp evaluate_rule(rule, storage, storage_mod, delta) do
    ordered_body = JoinOrder.reorder(rule.body, %{}, storage, storage_mod)
    delta_predicates = Map.keys(delta)

    # Find body goal indices whose predicate appears in the delta
    delta_indices =
      ordered_body
      |> Enum.with_index()
      |> Enum.filter(fn {{pred, _}, _idx} -> pred in delta_predicates end)
      |> Enum.map(fn {_, idx} -> idx end)

    if delta_indices == [] do
      # No body goals reference delta predicates — skip
      {[], storage}
    else
      # Semi-naive: for each delta index, evaluate a variant using delta for that goal
      indexed_body = Enum.with_index(ordered_body)

      all_facts =
        Enum.flat_map(delta_indices, fn delta_idx ->
          bindings =
            evaluate_body_with_delta(indexed_body, delta_idx, delta, storage, storage_mod, [%{}])

          bindings = filter_negations(bindings, rule.negations, storage, storage_mod)
          bindings = apply_guards(bindings, rule.guards)
          project_bindings(rule, bindings)
        end)
        |> Enum.uniq()

      {all_facts, storage}
    end
  end

  # Full evaluation (initial pass, no delta)
  defp evaluate_rule_full(rule, storage, storage_mod) do
    ordered_body = JoinOrder.reorder(rule.body, %{}, storage, storage_mod)

    bindings =
      if Leapfrog.eligible?(ordered_body) do
        Leapfrog.evaluate_join(ordered_body, storage, storage_mod)
      else
        evaluate_body(ordered_body, storage, storage_mod, [%{}])
      end

    bindings = filter_negations(bindings, rule.negations, storage, storage_mod)
    bindings = apply_guards(bindings, rule.guards)
    facts = project_bindings(rule, bindings)

    {facts, storage}
  end

  # Project bindings to facts via head instantiation or aggregation
  defp project_bindings(rule, bindings) do
    if rule.aggregations != [] do
      apply_aggregations(rule, bindings)
    else
      bindings
      |> Enum.map(fn binding -> instantiate_head(rule.head, binding) end)
      |> Enum.uniq()
    end
  end

  # Apply aggregation: group bindings, compute aggregate per group, produce facts
  defp apply_aggregations(rule, bindings) do
    rule.aggregations
    |> Enum.flat_map(&compute_aggregate(&1, bindings, rule.head))
    |> Enum.uniq()
  end

  defp compute_aggregate({agg_fn, target_var, source_var, group_vars}, bindings, head) do
    groups =
      Enum.group_by(bindings, fn binding ->
        Enum.map(group_vars, &Map.get(binding, &1))
      end)

    Enum.map(groups, fn {group_key, group_bindings} ->
      values = extract_agg_values(source_var, group_bindings)
      agg_value = Aggregation.compute(agg_fn, values, [])

      group_binding =
        Enum.zip(group_vars, group_key)
        |> Map.new()
        |> Map.put(target_var, agg_value)

      instantiate_head(head, group_binding)
    end)
  end

  defp extract_agg_values(:_, group_bindings), do: group_bindings

  defp extract_agg_values(source_var, group_bindings) do
    Enum.map(group_bindings, &Map.get(&1, source_var))
  end

  # Evaluate body goals with one goal using delta facts (semi-naive)
  defp evaluate_body_with_delta([], _delta_idx, _delta, _storage, _storage_mod, bindings) do
    bindings
  end

  defp evaluate_body_with_delta(
         [{{predicate, terms}, idx} | rest],
         delta_idx,
         delta,
         storage,
         storage_mod,
         bindings
       ) do
    new_bindings =
      if idx == delta_idx do
        # Use delta facts for this goal instead of full storage
        delta_facts = Map.get(delta, predicate, [])
        evaluate_goal_against_facts({predicate, terms}, delta_facts, bindings)
      else
        # Use full storage for all other goals
        Enum.flat_map(bindings, fn binding ->
          evaluate_goal({predicate, terms}, storage, storage_mod, binding)
        end)
      end

    evaluate_body_with_delta(rest, delta_idx, delta, storage, storage_mod, new_bindings)
  end

  # Evaluate a goal against a list of in-memory facts (the delta)
  defp evaluate_goal_against_facts({_predicate, terms}, delta_facts, bindings) do
    Enum.flat_map(bindings, fn binding ->
      pattern = Enum.map(terms, fn term -> Unification.substitute(term, binding) end)
      unify_matching_facts(terms, delta_facts, pattern, binding)
    end)
  end

  defp unify_matching_facts(terms, delta_facts, pattern, binding) do
    delta_facts
    |> Enum.filter(fn {_pred, tuple} -> matches_pattern?(tuple, pattern) end)
    |> Enum.flat_map(fn {_pred, tuple} ->
      case Unification.unify(terms, tuple, binding) do
        {:ok, new_binding} -> [new_binding]
        :fail -> []
      end
    end)
  end

  defp matches_pattern?(tuple, pattern) when length(tuple) == length(pattern) do
    Enum.zip(tuple, pattern)
    |> Enum.all?(fn
      {_, :_} -> true
      {a, a} -> true
      {_, _} -> false
    end)
  end

  defp matches_pattern?(_, _), do: false

  # Evaluate body goals, building up bindings
  defp evaluate_body([], _storage, _storage_mod, bindings), do: bindings

  defp evaluate_body([goal | rest], storage, storage_mod, bindings) do
    new_bindings =
      Enum.flat_map(bindings, fn binding ->
        evaluate_goal(goal, storage, storage_mod, binding)
      end)

    evaluate_body(rest, storage, storage_mod, new_bindings)
  end

  # Evaluate a single goal against current bindings
  defp evaluate_goal({predicate, terms}, storage, storage_mod, binding) do
    # Substitute known bindings into pattern
    pattern = Enum.map(terms, fn term -> Unification.substitute(term, binding) end)

    # Query storage
    {:ok, results} = storage_mod.lookup(storage, predicate, pattern)

    # Unify results with pattern to extend bindings
    Enum.flat_map(results, fn tuple ->
      case Unification.unify(terms, tuple, binding) do
        {:ok, new_binding} -> [new_binding]
        :fail -> []
      end
    end)
  end

  # Filter bindings by checking negations don't match
  defp filter_negations(bindings, [], _storage, _storage_mod), do: bindings

  defp filter_negations(bindings, negations, storage, storage_mod) do
    Enum.filter(bindings, fn binding ->
      Enum.all?(negations, &negation_absent?(&1, binding, storage, storage_mod))
    end)
  end

  defp negation_absent?({pred, terms}, binding, storage, storage_mod) do
    pattern = Enum.map(terms, fn term -> Unification.substitute(term, binding) end)
    {:ok, results} = storage_mod.lookup(storage, pred, pattern)
    Enum.empty?(results)
  end

  # Apply guards: first extend bindings with assignment guards, then filter
  defp apply_guards(bindings, []), do: bindings

  defp apply_guards(bindings, guards) do
    {assignment_guards, filter_guards} =
      Enum.split_with(guards, fn
        {:=, _var, _expr} -> true
        {:func, _target, _name, _args} -> true
        _ -> false
      end)

    bindings
    |> apply_assignment_guards(assignment_guards)
    |> filter_guards(filter_guards)
  end

  defp apply_assignment_guards(bindings, []), do: bindings

  defp apply_assignment_guards(bindings, assignment_guards) do
    Enum.map(bindings, fn binding ->
      Enum.reduce(assignment_guards, binding, &apply_single_assignment/2)
    end)
  end

  defp apply_single_assignment({:=, var, expr}, b) do
    if Map.has_key?(b, var), do: b, else: Map.put(b, var, eval_expr(expr, b))
  end

  defp apply_single_assignment({:func, target, func_name, arg_vars}, b) do
    if Map.has_key?(b, target) do
      b
    else
      args = Enum.map(arg_vars, fn v -> eval_expr(v, b) end)
      Map.put(b, target, Datalox.Functions.call(func_name, args))
    end
  end

  defp filter_guards(bindings, []), do: bindings

  defp filter_guards(bindings, guards) do
    Enum.filter(bindings, fn binding ->
      Enum.all?(guards, &evaluate_guard(&1, binding))
    end)
  end

  defp evaluate_guard({op, left, right}, binding) do
    l = eval_expr(left, binding)
    r = eval_expr(right, binding)
    apply_comparison(op, l, r)
  rescue
    ArithmeticError -> false
  end

  defp eval_expr(v, binding) when is_atom(v) do
    case Map.fetch(binding, v) do
      {:ok, val} -> val
      :error -> v
    end
  end

  defp eval_expr(n, _binding) when is_number(n), do: n
  defp eval_expr(s, _binding) when is_binary(s), do: s
  defp eval_expr({:+, a, b}, binding), do: eval_expr(a, binding) + eval_expr(b, binding)
  defp eval_expr({:-, a, b}, binding), do: eval_expr(a, binding) - eval_expr(b, binding)
  defp eval_expr({:*, a, b}, binding), do: eval_expr(a, binding) * eval_expr(b, binding)
  defp eval_expr({:/, a, b}, binding), do: eval_expr(a, binding) / eval_expr(b, binding)

  defp apply_comparison(:>, l, r), do: l > r
  defp apply_comparison(:<, l, r), do: l < r
  defp apply_comparison(:>=, l, r), do: l >= r
  defp apply_comparison(:<=, l, r), do: l <= r
  defp apply_comparison(:!=, l, r), do: l != r
  defp apply_comparison(:=, l, r), do: l == r

  # Instantiate head with binding
  defp instantiate_head({predicate, terms}, binding) do
    values = Enum.map(terms, fn term -> substitute_value(term, binding) end)
    {predicate, values}
  end

  defp substitute_value(term, binding) when is_atom(term) do
    if Unification.variable?(term) do
      Map.fetch!(binding, term)
    else
      term
    end
  end

  defp substitute_value(term, _binding), do: term

  # Group rules by head predicate — rules with same head predicate must be
  # sequential, but different head predicates can run in parallel.
  defp group_independent_rules(rules) do
    rules
    |> Enum.group_by(fn rule -> elem(rule.head, 0) end)
    |> Map.values()
  end
end

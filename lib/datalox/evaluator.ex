defmodule Datalox.Evaluator do
  @moduledoc """
  Semi-naive evaluation for Datalog rules.

  Semi-naive evaluation is an optimization over naive evaluation that
  avoids redundant computation by only considering "new" facts from
  the previous iteration (delta facts).
  """

  alias Datalox.Optimizer.Stratifier
  alias Datalox.Rule

  @type fact :: {atom(), list()}
  @type storage :: any()

  @doc """
  Evaluates rules against the current fact base and returns derived facts.

  Returns `{:ok, derived_facts, updated_storage}` on success.
  """
  @spec evaluate([Rule.t()], storage(), module()) ::
          {:ok, [fact()], storage()} | {:error, term()}
  def evaluate(rules, storage, storage_mod) do
    case Stratifier.stratify(rules) do
      {:ok, strata} ->
        evaluate_strata(strata, storage, storage_mod, [])

      {:error, _} = error ->
        error
    end
  end

  # Evaluate each stratum in order
  defp evaluate_strata([], storage, _storage_mod, acc) do
    {:ok, acc, storage}
  end

  defp evaluate_strata([stratum | rest], storage, storage_mod, acc) do
    {derived, storage} = evaluate_stratum(stratum, storage, storage_mod)
    evaluate_strata(rest, storage, storage_mod, acc ++ derived)
  end

  # Evaluate a single stratum using semi-naive iteration
  defp evaluate_stratum(rules, storage, storage_mod) do
    # Initial pass - derive all facts we can
    {initial_delta, storage} = derive_all(rules, storage, storage_mod)

    # Iterate until fixpoint
    iterate_seminaive(rules, storage, storage_mod, initial_delta, initial_delta)
  end

  defp iterate_seminaive(_rules, storage, _storage_mod, _delta, all_derived)
       when map_size(all_derived) == 0 or all_derived == [] do
    {Map.values(all_derived) |> List.flatten(), storage}
  end

  defp iterate_seminaive(rules, storage, storage_mod, delta, _all_derived) when is_list(delta) do
    delta_map = Enum.group_by(delta, fn {pred, _} -> pred end)
    iterate_seminaive(rules, storage, storage_mod, delta_map, list_to_derived_map(delta))
  end

  defp iterate_seminaive(rules, storage, storage_mod, delta, all_derived) do
    # Derive new facts using delta
    {new_facts, storage} = derive_with_delta(rules, storage, storage_mod, delta)

    # Filter out facts we've already derived
    new_delta =
      new_facts
      |> Enum.reject(fn fact -> Map.has_key?(all_derived, fact) end)

    if Enum.empty?(new_delta) do
      {Map.keys(all_derived), storage}
    else
      new_derived =
        Enum.reduce(new_delta, all_derived, fn fact, acc -> Map.put(acc, fact, true) end)

      new_delta_map = Enum.group_by(new_delta, fn {pred, _} -> pred end)
      iterate_seminaive(rules, storage, storage_mod, new_delta_map, new_derived)
    end
  end

  defp list_to_derived_map(facts) do
    Enum.reduce(facts, %{}, fn fact, acc -> Map.put(acc, fact, true) end)
  end

  # Initial derivation pass
  defp derive_all(rules, storage, storage_mod) do
    Enum.reduce(rules, {[], storage}, fn rule, {facts, st} ->
      {new_facts, st} = evaluate_rule(rule, st, storage_mod, %{})
      {facts ++ new_facts, st}
    end)
  end

  # Derive using delta facts
  defp derive_with_delta(rules, storage, storage_mod, delta) do
    Enum.reduce(rules, {[], storage}, fn rule, {facts, st} ->
      {new_facts, st} = evaluate_rule(rule, st, storage_mod, delta)
      {facts ++ new_facts, st}
    end)
  end

  # Evaluate a single rule
  defp evaluate_rule(rule, storage, storage_mod, _delta) do
    # Get all bindings from the body
    bindings = evaluate_body(rule.body, storage, storage_mod, [%{}])

    # Filter by negations
    bindings = filter_negations(bindings, rule.negations, storage, storage_mod)

    # Project to head variables and create facts
    facts =
      bindings
      |> Enum.map(fn binding -> instantiate_head(rule.head, binding) end)
      |> Enum.uniq()

    # Store derived facts
    storage =
      Enum.reduce(facts, storage, fn {pred, tuple}, st ->
        {:ok, st} = storage_mod.insert(st, pred, tuple)
        st
      end)

    {facts, storage}
  end

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
    pattern = Enum.map(terms, fn term -> substitute(term, binding) end)

    # Query storage
    {:ok, results} = storage_mod.lookup(storage, predicate, pattern)

    # Unify results with pattern to extend bindings
    Enum.flat_map(results, fn tuple ->
      case unify(terms, tuple, binding) do
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
    pattern = Enum.map(terms, fn term -> substitute(term, binding) end)
    {:ok, results} = storage_mod.lookup(storage, pred, pattern)
    Enum.empty?(results)
  end

  # Substitute variables in term using binding
  defp substitute(term, binding) when is_atom(term) do
    if variable?(term) do
      Map.get(binding, term, :_)
    else
      term
    end
  end

  defp substitute(term, _binding), do: term

  # Unify terms with values, extending binding
  defp unify([], [], binding), do: {:ok, binding}

  defp unify([term | terms], [value | values], binding) do
    case unify_one(term, value, binding) do
      {:ok, new_binding} -> unify(terms, values, new_binding)
      :fail -> :fail
    end
  end

  defp unify_one(:_, _value, binding), do: {:ok, binding}

  defp unify_one(term, value, binding) when is_atom(term) do
    if variable?(term) do
      case Map.fetch(binding, term) do
        {:ok, ^value} -> {:ok, binding}
        {:ok, _other} -> :fail
        :error -> {:ok, Map.put(binding, term, value)}
      end
    else
      if term == value, do: {:ok, binding}, else: :fail
    end
  end

  defp unify_one(term, value, binding) do
    if term == value, do: {:ok, binding}, else: :fail
  end

  # Instantiate head with binding
  defp instantiate_head({predicate, terms}, binding) do
    values = Enum.map(terms, fn term -> substitute_value(term, binding) end)
    {predicate, values}
  end

  defp substitute_value(term, binding) when is_atom(term) do
    if variable?(term) do
      Map.fetch!(binding, term)
    else
      term
    end
  end

  defp substitute_value(term, _binding), do: term

  # Check if term is a variable (uppercase atom)
  defp variable?(term) when is_atom(term) do
    str = Atom.to_string(term)
    String.match?(str, ~r/^[A-Z]/)
  end
end

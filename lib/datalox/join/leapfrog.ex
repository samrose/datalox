defmodule Datalox.Join.Leapfrog do
  @moduledoc """
  Leapfrog Triejoin — worst-case optimal multi-way join.

  Uses `:gb_trees` sorted iterators for efficient intersection
  of multiple sorted relations sharing a common variable.
  """

  @doc """
  Check if a body is eligible for WCOJ: 3+ goals with at least one
  variable appearing in all of them.
  """
  @spec eligible?([{atom(), list()}]) :: boolean()
  def eligible?(body) when length(body) < 3, do: false

  def eligible?(body) do
    # Count how many goals each variable appears in
    var_goal_counts =
      body
      |> Enum.flat_map(fn {_pred, terms} ->
        terms
        |> Enum.filter(&variable?/1)
        |> Enum.reject(&(&1 == :_))
        |> Enum.uniq()
      end)
      |> Enum.frequencies()

    # Eligible if any variable appears in 2+ goals (with 3+ total goals)
    Enum.any?(var_goal_counts, fn {_var, count} -> count >= 2 end)
  end

  @doc """
  Intersect multiple `:gb_trees`, returning all common keys in sorted order.
  """
  @spec intersect([:gb_trees.tree()]) :: [any()]
  def intersect(trees) when length(trees) < 2 do
    raise ArgumentError, "Need 2+ relations"
  end

  def intersect(trees) do
    # Initialize iterators with current position
    iters =
      trees
      |> Enum.map(fn tree -> {tree, :gb_trees.iterator(tree)} end)
      |> Enum.map(fn {tree, iter} ->
        case :gb_trees.next(iter) do
          {key, _val, next_iter} -> {key, next_iter, tree}
          :none -> nil
        end
      end)

    if Enum.any?(iters, &is_nil/1) do
      []
    else
      do_leapfrog(iters, [])
      |> Enum.reverse()
    end
  end

  @doc """
  Evaluate a multi-way join using leapfrog for the shared variable,
  then nested loop for remaining bindings.
  """
  @spec evaluate_join([{atom(), list()}], any(), module()) :: [map()]
  def evaluate_join(body, storage, storage_mod) do
    # Find the variable that appears in most goals
    shared_var = find_best_shared_var(body)

    # Split goals into those containing the shared var (for leapfrog) and others
    {shared_goals, other_goals} =
      Enum.split_with(body, fn {_pred, terms} ->
        Enum.any?(terms, &(&1 == shared_var))
      end)

    # Build sorted trees of distinct values for the shared variable
    trees =
      Enum.map(shared_goals, fn {pred, terms} ->
        col = Enum.find_index(terms, &(&1 == shared_var))
        {:ok, all_tuples} = storage_mod.all(storage, pred)

        all_tuples
        |> Enum.map(fn tuple -> Enum.at(tuple, col) end)
        |> Enum.uniq()
        |> Enum.sort()
        |> Enum.map(fn val -> {val, true} end)
        |> :gb_trees.from_orddict()
      end)

    # Intersect to get common values for the shared variable
    common_values =
      if Enum.any?(trees, &(:gb_trees.size(&1) == 0)) do
        []
      else
        intersect(trees)
      end

    # For each common value, do nested loop join on all goals
    Enum.flat_map(common_values, fn val ->
      initial_binding = %{shared_var => val}
      nested_loop_join(shared_goals ++ other_goals, storage, storage_mod, [initial_binding])
    end)
  end

  # Standard nested loop join starting from initial bindings
  defp nested_loop_join([], _storage, _storage_mod, bindings), do: bindings

  defp nested_loop_join([{pred, terms} | rest], storage, storage_mod, bindings) do
    new_bindings =
      Enum.flat_map(bindings, &extend_binding(&1, pred, terms, storage, storage_mod))

    nested_loop_join(rest, storage, storage_mod, new_bindings)
  end

  defp extend_binding(binding, pred, terms, storage, storage_mod) do
    pattern = Enum.map(terms, fn term -> substitute(term, binding) end)
    {:ok, results} = storage_mod.lookup(storage, pred, pattern)

    Enum.flat_map(results, fn tuple ->
      case unify(terms, tuple, binding) do
        {:ok, new_binding} -> [new_binding]
        :fail -> []
      end
    end)
  end

  defp substitute(term, binding) when is_atom(term) do
    if variable?(term), do: Map.get(binding, term, :_), else: term
  end

  defp substitute(term, _binding), do: term

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

  # Leapfrog core: advance iterators seeking matches
  defp do_leapfrog(iters, acc) do
    sorted = Enum.sort_by(iters, fn {key, _iter, _tree} -> key end)
    {min_key, _min_iter, _min_tree} = hd(sorted)
    {max_key, _max_iter, _max_tree} = List.last(sorted)

    if min_key == max_key,
      do: leapfrog_advance_all(sorted, min_key, acc),
      else: leapfrog_seek(sorted, max_key, acc)
  end

  defp leapfrog_advance_all(sorted, matched_key, acc) do
    advanced = Enum.map(sorted, &advance_iter/1)

    if Enum.any?(advanced, &is_nil/1),
      do: [matched_key | acc],
      else: do_leapfrog(advanced, [matched_key | acc])
  end

  defp advance_iter({_k, iter, tree}) do
    case :gb_trees.next(iter) do
      {key, _val, next_iter} -> {key, next_iter, tree}
      :none -> nil
    end
  end

  defp leapfrog_seek(sorted, max_key, acc) do
    {_k, _iter, min_tree} = hd(sorted)
    rest = tl(sorted)

    case seek(min_tree, max_key) do
      nil -> acc
      {found_key, found_iter} -> do_leapfrog([{found_key, found_iter, min_tree} | rest], acc)
    end
  end

  # Seek to the first key >= target in the tree
  defp seek(tree, target) do
    iter = :gb_trees.iterator_from(target, tree)

    case :gb_trees.next(iter) do
      {key, _val, next_iter} -> {key, next_iter}
      :none -> nil
    end
  end

  defp find_best_shared_var(body) do
    # Count occurrences of each variable across goals
    var_counts =
      body
      |> Enum.flat_map(fn {_pred, terms} ->
        terms |> Enum.filter(&variable?/1) |> Enum.reject(&(&1 == :_))
      end)
      |> Enum.frequencies()

    # Return the variable appearing in the most goals
    {var, _count} = Enum.max_by(var_counts, fn {_var, count} -> count end)
    var
  end

  defp variable?(term) when is_atom(term) do
    str = Atom.to_string(term)
    String.match?(str, ~r/^[A-Z]/)
  end

  defp variable?(_), do: false
end

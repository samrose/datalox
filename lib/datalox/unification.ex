defmodule Datalox.Unification do
  @moduledoc """
  Shared unification primitives for Datalox.

  Provides variable detection, term substitution, and pattern unification
  used across the evaluator, join algorithms, incremental maintenance,
  and safety checker.
  """

  @doc """
  Returns true if `term` is a Datalog variable (atom starting with uppercase A-Z).
  """
  @spec variable?(any()) :: boolean()
  def variable?(term) when is_atom(term) do
    case Atom.to_string(term) do
      <<c, _::binary>> when c >= ?A and c <= ?Z -> true
      _ -> false
    end
  end

  def variable?(_), do: false

  @doc """
  Substitute variables in `term` using `binding`.

  Bound variables are replaced with their values.
  Unbound variables become `:_` (wildcard).
  Non-variable terms are returned unchanged.
  """
  @spec substitute(any(), map()) :: any()
  def substitute(term, binding) when is_atom(term) do
    if variable?(term), do: Map.get(binding, term, :_), else: term
  end

  def substitute(term, _binding), do: term

  @doc """
  Unify a list of terms with a list of values, extending `binding`.

  Returns `{:ok, extended_binding}` on success, or `:fail` if terms
  and values are incompatible.
  """
  @spec unify([any()], [any()], map()) :: {:ok, map()} | :fail
  def unify([], [], binding), do: {:ok, binding}

  def unify([term | terms], [value | values], binding) do
    case unify_one(term, value, binding) do
      {:ok, new_binding} -> unify(terms, values, new_binding)
      :fail -> :fail
    end
  end

  def unify(_, _, _), do: :fail

  @doc """
  Unify a single term with a value, extending `binding`.

  - `:_` (wildcard) matches anything without extending the binding
  - Variables bind to values or must match their existing binding
  - Constants must match exactly
  """
  @spec unify_one(any(), any(), map()) :: {:ok, map()} | :fail
  def unify_one(:_, _value, binding), do: {:ok, binding}

  def unify_one(term, value, binding) when is_atom(term) do
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

  def unify_one(term, value, binding) do
    if term == value, do: {:ok, binding}, else: :fail
  end
end

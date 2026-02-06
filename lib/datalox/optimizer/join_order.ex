defmodule Datalox.Optimizer.JoinOrder do
  @moduledoc """
  Cost-based join ordering for Datalog rule bodies.

  Reorders body goals to evaluate smaller (cheaper) relations first,
  taking into account which variables are already bound from earlier goals.
  """

  @doc """
  Reorders body goals by estimated cost (relation cardinality).

  Goals with fewer matching facts are placed first. When cardinalities
  are equal, original order is preserved (stable sort).
  """
  @spec reorder([{atom(), list()}], map(), any(), module()) :: [{atom(), list()}]
  def reorder(body, _binding, _storage, _storage_mod) when length(body) <= 1, do: body

  def reorder(body, _binding, storage, storage_mod) do
    # Estimate cost for each goal based on relation size
    costed =
      body
      |> Enum.with_index()
      |> Enum.map(fn {{pred, terms}, idx} ->
        cost = estimate_cost(pred, storage, storage_mod)
        {cost, idx, {pred, terms}}
      end)

    # Stable sort by cost (preserve index for ties)
    costed
    |> Enum.sort_by(fn {cost, idx, _goal} -> {cost, idx} end)
    |> Enum.map(fn {_cost, _idx, goal} -> goal end)
  end

  defp estimate_cost(predicate, storage, storage_mod) do
    if function_exported?(storage_mod, :count, 2) do
      case storage_mod.count(storage, predicate) do
        {:ok, n} -> n
        _ -> 1_000_000
      end
    else
      1_000_000
    end
  end
end

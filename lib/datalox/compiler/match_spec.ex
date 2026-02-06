defmodule Datalox.Compiler.MatchSpec do
  @moduledoc "Compile Datalog patterns to ETS match specifications."

  @doc """
  Compile a pattern (list of values or :_ wildcards) into an ETS match spec.

  ETS stores records as `{key, [col0, col1, ...]}` in duplicate_bag tables.
  The match spec matches on the list elements and returns the full record.
  """
  @spec compile(list(), non_neg_integer()) :: :ets.match_spec()
  def compile(pattern, arity) do
    # Build match variables for each column position: :"$1", :"$2", ...
    vars = for i <- 1..arity, do: :"$#{i}"

    # Match head: {any_key, [:"$1", :"$2", ...]}
    match_head = {:_, vars}

    # Build guards from bound positions in pattern
    guards =
      pattern
      |> Enum.with_index(1)
      |> Enum.flat_map(fn
        {:_, _idx} -> []
        {val, idx} -> [{:==, :"$#{idx}", val}]
      end)

    # Return the full record
    result = [:"$_"]

    [{match_head, guards, result}]
  end
end

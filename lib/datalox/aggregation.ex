defmodule Datalox.Aggregation do
  @moduledoc """
  Aggregation functions for Datalog queries.

  Supports: count, sum, min, max, avg, collect
  """

  @type aggregate_fn :: :count | :sum | :min | :max | :avg | :collect

  @doc """
  Computes an aggregate over a list of values.
  """
  @spec compute(aggregate_fn(), [any()], keyword()) :: any()
  def compute(:count, values, _opts), do: length(values)

  def compute(:sum, [], _opts), do: 0
  def compute(:sum, values, _opts), do: Enum.sum(values)

  def compute(:min, [], _opts), do: nil
  def compute(:min, values, _opts), do: Enum.min(values)

  def compute(:max, [], _opts), do: nil
  def compute(:max, values, _opts), do: Enum.max(values)

  def compute(:avg, [], _opts), do: nil
  def compute(:avg, values, _opts), do: Enum.sum(values) / length(values)

  def compute(:collect, values, _opts), do: values
end

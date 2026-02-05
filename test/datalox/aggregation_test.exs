defmodule Datalox.AggregationTest do
  use ExUnit.Case, async: true

  alias Datalox.Aggregation

  describe "compute/3" do
    test "computes count" do
      values = [1, 2, 3, 4, 5]
      assert Aggregation.compute(:count, values, []) == 5
    end

    test "computes sum" do
      values = [10, 20, 30]
      assert Aggregation.compute(:sum, values, []) == 60
    end

    test "computes min" do
      values = [5, 2, 8, 1, 9]
      assert Aggregation.compute(:min, values, []) == 1
    end

    test "computes max" do
      values = [5, 2, 8, 1, 9]
      assert Aggregation.compute(:max, values, []) == 9
    end

    test "computes avg" do
      values = [10, 20, 30]
      assert Aggregation.compute(:avg, values, []) == 20.0
    end

    test "computes collect" do
      values = [1, 2, 3]
      result = Aggregation.compute(:collect, values, [])
      assert Enum.sort(result) == [1, 2, 3]
    end

    test "handles empty values for count" do
      assert Aggregation.compute(:count, [], []) == 0
    end

    test "handles empty values for sum" do
      assert Aggregation.compute(:sum, [], []) == 0
    end

    test "returns nil for min/max on empty" do
      assert Aggregation.compute(:min, [], []) == nil
      assert Aggregation.compute(:max, [], []) == nil
    end
  end
end

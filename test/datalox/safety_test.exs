defmodule Datalox.SafetyTest do
  use ExUnit.Case, async: true
  alias Datalox.{Rule, Safety}

  test "valid rule passes" do
    rule = Rule.new({:ancestor, [:X, :Y]}, [{:parent, [:X, :Y]}])
    assert :ok = Safety.check_all([rule])
  end

  test "head variable not in body fails" do
    rule = Rule.new({:result, [:X, :Z]}, [{:parent, [:X, :Y]}])
    assert {:error, errors} = Safety.check_all([rule])
    assert Enum.any?(errors, &String.contains?(&1, "Z"))
  end

  test "negation variable not in positive body fails" do
    rule = Rule.new({:active, [:X]}, [{:user, [:X]}], negations: [{:banned, [:Y]}])
    assert {:error, errors} = Safety.check_all([rule])
    assert Enum.any?(errors, &String.contains?(&1, "Y"))
  end

  test "aggregation group var not in body fails" do
    rule =
      Rule.new(
        {:count_dept, [:D, :N]},
        [{:employee, [:_, :D]}],
        aggregations: [{:count, :N, :_, [:D, :Z]}]
      )

    assert {:error, errors} = Safety.check_all([rule])
    assert Enum.any?(errors, &String.contains?(&1, "Z"))
  end

  test "wildcard in head fails" do
    rule = Rule.new({:result, [:X, :_]}, [{:parent, [:X, :Y]}])
    assert {:error, errors} = Safety.check_all([rule])
    assert Enum.any?(errors, &String.contains?(&1, "wildcard"))
  end

  test "multiple rules — first valid, second invalid" do
    r1 = Rule.new({:ancestor, [:X, :Y]}, [{:parent, [:X, :Y]}])
    r2 = Rule.new({:bad, [:X, :Z]}, [{:parent, [:X, :Y]}])
    assert {:error, errors} = Safety.check_all([r1, r2])
    assert length(errors) == 1
    assert Enum.any?(errors, &String.contains?(&1, "Z"))
  end

  test "guard variable not in positive body fails" do
    rule =
      Rule.new({:result, [:X]}, [{:value, [:X]}],
        guards: [{:>, :Y, 5}]
      )

    assert {:error, errors} = Safety.check_all([rule])
    assert Enum.any?(errors, &String.contains?(&1, "Y"))
  end

  test "all variables bound passes with guards" do
    rule =
      Rule.new({:high, [:X, :V]}, [{:score, [:X, :V]}],
        guards: [{:>, :V, 50}]
      )

    assert :ok = Safety.check_all([rule])
  end
end

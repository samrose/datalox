defmodule Datalox.FunctionsTest do
  use ExUnit.Case, async: false
  alias Datalox.{Evaluator, Functions, Rule}
  alias Datalox.Storage.ETS

  setup do
    {:ok, storage} = ETS.init(name: :"funcs_#{:erlang.unique_integer()}")
    on_exit(fn -> ETS.terminate(storage) end)

    Functions.register(:double, fn [x] -> x * 2 end)
    Functions.register(:upcase, fn [s] -> String.upcase(s) end)

    {:ok, storage: storage}
  end

  test "function call in rule body", %{storage: storage} do
    {:ok, storage} = ETS.insert(storage, :value, ["a", 5])

    rules = [
      Rule.new({:doubled, [:K, :D]}, [{:value, [:K, :V]}],
        guards: [{:func, :D, :double, [:V]}]
      )
    ]

    {:ok, derived, _} = Evaluator.evaluate(rules, storage, ETS)
    assert {:doubled, ["a", 10]} in derived
  end

  test "string function call", %{storage: storage} do
    {:ok, storage} = ETS.insert(storage, :name, ["alice"])

    rules = [
      Rule.new({:upper_name, [:U]}, [{:name, [:N]}],
        guards: [{:func, :U, :upcase, [:N]}]
      )
    ]

    {:ok, derived, _} = Evaluator.evaluate(rules, storage, ETS)
    assert {:upper_name, ["ALICE"]} in derived
  end

  test "register and check existence" do
    Functions.register(:test_fn, fn [x] -> x end)
    assert Functions.registered?(:test_fn)
    refute Functions.registered?(:nonexistent)
  end

  test "function call returns correct result" do
    Functions.register(:add_one, fn [x] -> x + 1 end)
    assert Functions.call(:add_one, [5]) == 6
  end
end

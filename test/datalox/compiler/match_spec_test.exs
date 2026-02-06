defmodule Datalox.Compiler.MatchSpecTest do
  use ExUnit.Case, async: true
  alias Datalox.Compiler.MatchSpec

  test "compiles simple pattern to match spec" do
    ms = MatchSpec.compile([:_, "alice"], 2)
    assert is_list(ms)
    assert length(ms) == 1
  end

  test "compiled match spec works with ETS select for second column" do
    table = :ets.new(:test_ms1, [:duplicate_bag, :public])
    :ets.insert(table, {"alice", ["alice", "bob"]})
    :ets.insert(table, {"carol", ["carol", "bob"]})
    :ets.insert(table, {"dave", ["dave", "eve"]})

    ms = MatchSpec.compile([:_, "bob"], 2)
    results = :ets.select(table, ms)
    assert length(results) == 2

    tuples = Enum.map(results, fn {_key, tuple} -> tuple end)
    assert ["alice", "bob"] in tuples
    assert ["carol", "bob"] in tuples
    :ets.delete(table)
  end

  test "compiled match spec with first column bound" do
    table = :ets.new(:test_ms2, [:duplicate_bag, :public])
    :ets.insert(table, {"alice", ["alice", "bob"]})
    :ets.insert(table, {"alice", ["alice", "carol"]})
    :ets.insert(table, {"dave", ["dave", "eve"]})

    ms = MatchSpec.compile(["alice", :_], 2)
    results = :ets.select(table, ms)
    assert length(results) == 2
    :ets.delete(table)
  end

  test "compiled match spec all wildcards returns all" do
    table = :ets.new(:test_ms3, [:duplicate_bag, :public])
    :ets.insert(table, {"a", ["a", "b"]})
    :ets.insert(table, {"c", ["c", "d"]})

    ms = MatchSpec.compile([:_, :_], 2)
    results = :ets.select(table, ms)
    assert length(results) == 2
    :ets.delete(table)
  end

  test "compiled match spec with all columns bound" do
    table = :ets.new(:test_ms4, [:duplicate_bag, :public])
    :ets.insert(table, {"a", ["a", "b"]})
    :ets.insert(table, {"a", ["a", "c"]})

    ms = MatchSpec.compile(["a", "b"], 2)
    results = :ets.select(table, ms)
    assert length(results) == 1

    [{_key, tuple}] = results
    assert tuple == ["a", "b"]
    :ets.delete(table)
  end

  test "3-arity pattern" do
    table = :ets.new(:test_ms5, [:duplicate_bag, :public])
    :ets.insert(table, {"a", ["a", "b", "c"]})
    :ets.insert(table, {"d", ["d", "b", "e"]})
    :ets.insert(table, {"f", ["f", "g", "c"]})

    ms = MatchSpec.compile([:_, "b", :_], 3)
    results = :ets.select(table, ms)
    assert length(results) == 2
    :ets.delete(table)
  end
end

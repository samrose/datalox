defmodule Datalox.AggregationIntegrationTest do
  use ExUnit.Case, async: true

  setup do
    name = :"agg_integration_#{:erlang.unique_integer()}"
    {:ok, db} = Datalox.new(name: name)

    on_exit(fn ->
      try do
        Datalox.stop(db)
      catch
        :exit, _ -> :ok
      end
    end)

    {:ok, db: db}
  end

  test "aggregation from .dl file produces correct counts", %{db: db} do
    :ok = Datalox.load_file(db, "test/fixtures/aggregation.dl")

    results = Datalox.query(db, {:dept_size, [:_, :_]})

    eng = Enum.find(results, fn {:dept_size, [dept, _]} -> dept == "engineering" end)
    sales = Enum.find(results, fn {:dept_size, [dept, _]} -> dept == "sales" end)

    assert {:dept_size, ["engineering", 2]} = eng
    assert {:dept_size, ["sales", 3]} = sales
  end

  test "aggregation via programmatic API", %{db: db} do
    Datalox.assert(db, {:sale, ["alice", 100]})
    Datalox.assert(db, {:sale, ["alice", 250]})
    Datalox.assert(db, {:sale, ["bob", 75]})

    alias Datalox.{Database, Rule}

    rules = [
      Rule.new(
        {:total_sales, [:Person, :Total]},
        [{:sale, [:Person, :Amount]}],
        aggregations: [{:sum, :Total, :Amount, [:Person]}]
      )
    ]

    Database.load_rules(db, rules)

    results = Datalox.query(db, {:total_sales, [:_, :_]})
    assert {:total_sales, ["alice", 350]} in results
    assert {:total_sales, ["bob", 75]} in results
  end
end

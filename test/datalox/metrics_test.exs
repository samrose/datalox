defmodule Datalox.MetricsTest do
  use ExUnit.Case, async: true

  alias Datalox.Metrics

  describe "attach/0 and query_stats/1" do
    test "tracks query metrics" do
      name = :"test_#{:erlang.unique_integer()}"
      {:ok, db} = Datalox.new(name: name)

      Datalox.assert(db, {:user, ["alice", :admin]})
      Datalox.query(db, {:user, [:_, :_]})
      Datalox.query(db, {:user, [:_, :admin]})

      stats = Metrics.get_stats(db)

      assert stats.total_queries >= 2
      assert is_map(stats.facts)

      Datalox.stop(db)
    end
  end
end

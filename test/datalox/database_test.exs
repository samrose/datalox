defmodule Datalox.DatabaseTest do
  use ExUnit.Case, async: true

  alias Datalox.Database

  setup do
    name = :"test_db_#{:erlang.unique_integer()}"
    {:ok, db} = Database.start_link(name: name)
    {:ok, db: db}
  end

  describe "assert/2 and query/2" do
    test "asserts and queries a fact", %{db: db} do
      :ok = Database.assert(db, {:user, ["alice", :admin]})
      results = Database.query(db, {:user, [:_, :_]})

      assert results == [{:user, ["alice", :admin]}]
    end

    test "queries with pattern matching", %{db: db} do
      :ok = Database.assert(db, {:user, ["alice", :admin]})
      :ok = Database.assert(db, {:user, ["bob", :viewer]})

      results = Database.query(db, {:user, [:_, :admin]})
      assert results == [{:user, ["alice", :admin]}]
    end

    test "returns empty list for no matches", %{db: db} do
      :ok = Database.assert(db, {:user, ["alice", :admin]})
      results = Database.query(db, {:user, [:_, :nobody]})

      assert results == []
    end
  end

  describe "retract/2" do
    test "removes a fact", %{db: db} do
      :ok = Database.assert(db, {:user, ["alice", :admin]})
      :ok = Database.retract(db, {:user, ["alice", :admin]})

      results = Database.query(db, {:user, [:_, :_]})
      assert results == []
    end
  end

  describe "exists?/2" do
    test "returns true when fact exists", %{db: db} do
      :ok = Database.assert(db, {:user, ["alice", :admin]})
      assert Database.exists?(db, {:user, ["alice", :admin]})
    end

    test "returns false when fact does not exist", %{db: db} do
      refute Database.exists?(db, {:user, ["alice", :admin]})
    end

    test "supports pattern matching", %{db: db} do
      :ok = Database.assert(db, {:user, ["alice", :admin]})
      assert Database.exists?(db, {:user, [:_, :admin]})
      refute Database.exists?(db, {:user, [:_, :viewer]})
    end
  end

  describe "load_rules/2 and rule evaluation" do
    test "evaluates rules on query", %{db: db} do
      # Add base facts
      :ok = Database.assert(db, {:parent, ["alice", "bob"]})
      :ok = Database.assert(db, {:parent, ["bob", "carol"]})

      # Add rules
      rules = [
        Datalox.Rule.new({:ancestor, [:X, :Y]}, [{:parent, [:X, :Y]}]),
        Datalox.Rule.new({:ancestor, [:X, :Z]}, [{:parent, [:X, :Y]}, {:ancestor, [:Y, :Z]}])
      ]

      :ok = Database.load_rules(db, rules)

      # Query should return derived facts
      results = Database.query(db, {:ancestor, [:_, :_]})

      assert {:ancestor, ["alice", "bob"]} in results
      assert {:ancestor, ["bob", "carol"]} in results
      assert {:ancestor, ["alice", "carol"]} in results
    end
  end
end

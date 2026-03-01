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

  describe "incremental derivation with recursive rules" do
    setup %{db: db} do
      rules = [
        Datalox.Rule.new({:ancestor, [:X, :Y]}, [{:parent, [:X, :Y]}]),
        Datalox.Rule.new({:ancestor, [:X, :Z]}, [
          {:parent, [:X, :Y]},
          {:ancestor, [:Y, :Z]}
        ])
      ]

      :ok = Database.load_rules(db, rules)
      {:ok, rules: rules}
    end

    test "derives transitive facts when asserting after rules loaded", %{db: db} do
      :ok = Database.assert(db, {:parent, ["alice", "bob"]})
      :ok = Database.assert(db, {:parent, ["bob", "carol"]})

      results = Database.query(db, {:ancestor, [:_, :_]})

      assert {:ancestor, ["alice", "bob"]} in results
      assert {:ancestor, ["bob", "carol"]} in results
      assert {:ancestor, ["alice", "carol"]} in results
    end

    test "derives deep transitive facts", %{db: db} do
      :ok = Database.assert(db, {:parent, ["a", "b"]})
      :ok = Database.assert(db, {:parent, ["b", "c"]})
      :ok = Database.assert(db, {:parent, ["c", "d"]})

      results = Database.query(db, {:ancestor, [:_, :_]})

      assert {:ancestor, ["a", "b"]} in results
      assert {:ancestor, ["a", "c"]} in results
      assert {:ancestor, ["a", "d"]} in results
      assert {:ancestor, ["b", "c"]} in results
      assert {:ancestor, ["b", "d"]} in results
      assert {:ancestor, ["c", "d"]} in results
    end

    test "retraction removes transitive derived facts", %{db: db} do
      :ok = Database.assert(db, {:parent, ["a", "b"]})
      :ok = Database.assert(db, {:parent, ["b", "c"]})

      # Verify transitive fact exists
      results = Database.query(db, {:ancestor, [:_, :_]})
      assert {:ancestor, ["a", "c"]} in results

      # Retract the middle link
      :ok = Database.retract(db, {:parent, ["b", "c"]})

      results = Database.query(db, {:ancestor, [:_, :_]})
      assert {:ancestor, ["a", "b"]} in results
      refute Enum.any?(results, fn {_, [_, y]} -> y == "c" end)
    end

    test "reload rules clears stale derived facts", %{db: db, rules: rules} do
      :ok = Database.assert(db, {:parent, ["a", "b"]})
      :ok = Database.assert(db, {:parent, ["b", "c"]})

      results = Database.query(db, {:ancestor, [:_, :_]})
      assert {:ancestor, ["a", "c"]} in results

      # Retract a fact and reload rules
      :ok = Database.retract(db, {:parent, ["b", "c"]})
      :ok = Database.load_rules(db, rules)

      results = Database.query(db, {:ancestor, [:_, :_]})
      assert {:ancestor, ["a", "b"]} in results
      refute {:ancestor, ["a", "c"]} in results
      refute {:ancestor, ["b", "c"]} in results
    end
  end

  describe "negation support" do
    setup %{db: db} do
      # active(X) :- user(X), not banned(X)
      rules = [
        Datalox.Rule.new({:active, [:X]}, [{:user, [:X]}], negations: [{:banned, [:X]}])
      ]

      :ok = Database.load_rules(db, rules)
      {:ok, rules: rules}
    end

    test "derives facts with negation", %{db: db} do
      :ok = Database.assert(db, {:user, ["alice"]})

      results = Database.query(db, {:active, [:_]})
      assert {:active, ["alice"]} in results
    end

    test "asserting negated fact retracts derived fact", %{db: db} do
      :ok = Database.assert(db, {:user, ["alice"]})
      :ok = Database.assert(db, {:user, ["bob"]})

      results = Database.query(db, {:active, [:_]})
      assert {:active, ["alice"]} in results
      assert {:active, ["bob"]} in results

      # Ban alice
      :ok = Database.assert(db, {:banned, ["alice"]})

      results = Database.query(db, {:active, [:_]})
      refute {:active, ["alice"]} in results
      assert {:active, ["bob"]} in results
    end

    test "retracting negated fact re-derives fact", %{db: db} do
      :ok = Database.assert(db, {:user, ["alice"]})
      :ok = Database.assert(db, {:banned, ["alice"]})

      results = Database.query(db, {:active, [:_]})
      refute {:active, ["alice"]} in results

      # Unban alice
      :ok = Database.retract(db, {:banned, ["alice"]})

      results = Database.query(db, {:active, [:_]})
      assert {:active, ["alice"]} in results
    end

    test "negation does not affect unrelated users", %{db: db} do
      :ok = Database.assert(db, {:user, ["alice"]})
      :ok = Database.assert(db, {:user, ["bob"]})
      :ok = Database.assert(db, {:banned, ["alice"]})

      results = Database.query(db, {:active, [:_]})
      refute {:active, ["alice"]} in results
      assert {:active, ["bob"]} in results

      :ok = Database.retract(db, {:banned, ["alice"]})

      results = Database.query(db, {:active, [:_]})
      assert {:active, ["alice"]} in results
      assert {:active, ["bob"]} in results
    end
  end
end

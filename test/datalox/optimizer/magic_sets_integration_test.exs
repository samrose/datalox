defmodule Datalox.Optimizer.MagicSetsIntegrationTest do
  use ExUnit.Case, async: true

  setup do
    name = :"magic_int_#{:erlang.unique_integer()}"
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

  test "query with bound first arg uses magic sets for recursive rules", %{db: db} do
    # Build a chain: a->b->c->d->e
    Datalox.assert(db, {:parent, ["a", "b"]})
    Datalox.assert(db, {:parent, ["b", "c"]})
    Datalox.assert(db, {:parent, ["c", "d"]})
    Datalox.assert(db, {:parent, ["d", "e"]})

    alias Datalox.{Database, Rule}

    rules = [
      Rule.new({:ancestor, [:X, :Y]}, [{:parent, [:X, :Y]}]),
      Rule.new({:ancestor, [:X, :Z]}, [{:parent, [:X, :Y]}, {:ancestor, [:Y, :Z]}])
    ]

    Database.load_rules(db, rules)

    # Query with bound first argument — should get all ancestors of "a"
    results = Datalox.query(db, {:ancestor, ["a", :_]})

    assert {:ancestor, ["a", "b"]} in results
    assert {:ancestor, ["a", "c"]} in results
    assert {:ancestor, ["a", "d"]} in results
    assert {:ancestor, ["a", "e"]} in results
  end

  test "query without bound args returns all results normally", %{db: db} do
    Datalox.assert(db, {:parent, ["a", "b"]})
    Datalox.assert(db, {:parent, ["b", "c"]})

    alias Datalox.{Database, Rule}

    rules = [
      Rule.new({:ancestor, [:X, :Y]}, [{:parent, [:X, :Y]}]),
      Rule.new({:ancestor, [:X, :Z]}, [{:parent, [:X, :Y]}, {:ancestor, [:Y, :Z]}])
    ]

    Database.load_rules(db, rules)

    # Query without bound args — should get all ancestors
    results = Datalox.query(db, {:ancestor, [:_, :_]})
    assert length(results) >= 3
  end

  test "non-recursive predicate query works normally", %{db: db} do
    Datalox.assert(db, {:fact, ["hello", "world"]})

    results = Datalox.query(db, {:fact, ["hello", :_]})
    assert {:fact, ["hello", "world"]} in results
  end
end

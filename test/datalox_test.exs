defmodule DataloxTest do
  use ExUnit.Case, async: true

  describe "new/1" do
    test "creates a new database" do
      name = :"test_#{:erlang.unique_integer()}"
      assert {:ok, db} = Datalox.new(name: name)
      assert is_pid(db)
      Datalox.stop(db)
    end

    test "requires name option" do
      assert_raise KeyError, fn ->
        Datalox.new([])
      end
    end
  end

  describe "assert/2 and query/2" do
    setup do
      name = :"test_#{:erlang.unique_integer()}"
      {:ok, db} = Datalox.new(name: name)
      on_exit(fn -> catch_exit(Datalox.stop(db)) end)
      {:ok, db: db}
    end

    test "basic fact operations", %{db: db} do
      :ok = Datalox.assert(db, {:user, ["alice", :admin]})
      results = Datalox.query(db, {:user, [:_, :_]})

      assert results == [{:user, ["alice", :admin]}]
    end
  end

  describe "assert_all/2" do
    setup do
      name = :"test_#{:erlang.unique_integer()}"
      {:ok, db} = Datalox.new(name: name)
      on_exit(fn -> catch_exit(Datalox.stop(db)) end)
      {:ok, db: db}
    end

    test "asserts multiple facts", %{db: db} do
      facts = [
        {:user, ["alice", :admin]},
        {:user, ["bob", :viewer]},
        {:role, [:admin, :write]}
      ]

      :ok = Datalox.assert_all(db, facts)

      users = Datalox.query(db, {:user, [:_, :_]})
      assert length(users) == 2
    end
  end

  describe "query_one/2" do
    setup do
      name = :"test_#{:erlang.unique_integer()}"
      {:ok, db} = Datalox.new(name: name)
      on_exit(fn -> catch_exit(Datalox.stop(db)) end)
      {:ok, db: db}
    end

    test "returns single result", %{db: db} do
      :ok = Datalox.assert(db, {:user, ["alice", :admin]})
      result = Datalox.query_one(db, {:user, ["alice", :_]})

      assert result == {:user, ["alice", :admin]}
    end

    test "returns nil for no matches", %{db: db} do
      result = Datalox.query_one(db, {:user, [:_, :_]})
      assert result == nil
    end
  end

  describe "exists?/2" do
    setup do
      name = :"test_#{:erlang.unique_integer()}"
      {:ok, db} = Datalox.new(name: name)
      on_exit(fn -> catch_exit(Datalox.stop(db)) end)
      {:ok, db: db}
    end

    test "checks fact existence", %{db: db} do
      refute Datalox.exists?(db, {:user, [:_, :_]})
      :ok = Datalox.assert(db, {:user, ["alice", :admin]})
      assert Datalox.exists?(db, {:user, [:_, :_]})
    end
  end
end

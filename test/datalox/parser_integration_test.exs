defmodule Datalox.ParserIntegrationTest do
  use ExUnit.Case, async: true

  describe "load_file/2" do
    test "loads facts and rules from a .dl file" do
      content = """
      % Family relationships
      parent("alice", "bob").
      parent("bob", "carol").

      % Ancestor rule
      ancestor(X, Y) :- parent(X, Y).
      ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z).
      """

      path = Path.join(System.tmp_dir!(), "test_#{:erlang.unique_integer()}.dl")
      File.write!(path, content)

      name = :"test_#{:erlang.unique_integer()}"
      {:ok, db} = Datalox.new(name: name)

      :ok = Datalox.load_file(db, path)

      results = Datalox.query(db, {:ancestor, [:_, :_]})

      assert {:ancestor, ["alice", "bob"]} in results
      assert {:ancestor, ["bob", "carol"]} in results
      assert {:ancestor, ["alice", "carol"]} in results

      Datalox.stop(db)
      File.rm!(path)
    end
  end
end

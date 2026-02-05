defmodule Datalox.DSLTest do
  use ExUnit.Case, async: true

  describe "deffact/1" do
    test "defines facts" do
      defmodule TestFacts do
        use Datalox.DSL

        deffact(user("alice", :admin))
        deffact(user("bob", :viewer))
      end

      facts = TestFacts.__datalox_facts__()

      assert {:user, ["alice", :admin]} in facts
      assert {:user, ["bob", :viewer]} in facts
    end
  end

  describe "defrule/2" do
    test "defines simple rules" do
      defmodule TestRules do
        use Datalox.DSL

        defrule ancestor(x, y) do
          parent(x, y)
        end
      end

      [rule] = TestRules.__datalox_rules__()

      assert rule.head == {:ancestor, [:X, :Y]}
      assert rule.body == [{:parent, [:X, :Y]}]
    end

    test "defines rules with multiple body goals" do
      defmodule TestMultiBody do
        use Datalox.DSL

        defrule can_access(user, resource) do
          user(user, role) and permission(role, resource)
        end
      end

      [rule] = TestMultiBody.__datalox_rules__()

      assert rule.head == {:can_access, [:USER, :RESOURCE]}
      assert length(rule.body) == 2
    end

    test "defines rules with negation" do
      defmodule TestNegation do
        use Datalox.DSL

        defrule active(user) do
          user(user) and not banned(user)
        end
      end

      [rule] = TestNegation.__datalox_rules__()

      assert rule.body == [{:user, [:USER]}]
      assert rule.negations == [{:banned, [:USER]}]
    end
  end

  describe "integration" do
    test "loads DSL module into database" do
      defmodule IntegrationRules do
        use Datalox.DSL

        deffact(parent("alice", "bob"))
        deffact(parent("bob", "carol"))

        defrule ancestor(x, y) do
          parent(x, y)
        end

        defrule ancestor(x, z) do
          parent(x, y) and ancestor(y, z)
        end
      end

      name = :"test_#{:erlang.unique_integer()}"
      {:ok, db} = Datalox.new(name: name)

      :ok = Datalox.load_module(db, IntegrationRules)

      results = Datalox.query(db, {:ancestor, [:_, :_]})

      assert {:ancestor, ["alice", "bob"]} in results
      assert {:ancestor, ["alice", "carol"]} in results

      Datalox.stop(db)
    end
  end
end

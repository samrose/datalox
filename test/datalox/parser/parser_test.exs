defmodule Datalox.Parser.ParserTest do
  use ExUnit.Case, async: true

  alias Datalox.Parser.Parser

  describe "parse/1" do
    test "parses a fact" do
      input = ~s|user("alice", admin).|
      {:ok, result} = Parser.parse(input)

      assert {:fact, {:user, ["alice", :admin]}} in result
    end

    test "parses multiple facts" do
      input = """
      user("alice", admin).
      user("bob", viewer).
      """

      {:ok, result} = Parser.parse(input)

      assert length(result) == 2
    end

    test "parses a simple rule" do
      input = ~s|ancestor(X, Y) :- parent(X, Y).|
      {:ok, result} = Parser.parse(input)

      [{:rule, rule}] = result
      assert rule.head == {:ancestor, [:X, :Y]}
      assert rule.body == [{:parent, [:X, :Y]}]
    end

    test "parses a rule with multiple body goals" do
      input = ~s|can_access(U, R) :- user(U, Role), permission(Role, R).|
      {:ok, result} = Parser.parse(input)

      [{:rule, rule}] = result
      assert length(rule.body) == 2
    end

    test "parses a rule with negation" do
      input = ~s|active(X) :- user(X), not banned(X).|
      {:ok, result} = Parser.parse(input)

      [{:rule, rule}] = result
      assert rule.negations == [{:banned, [:X]}]
    end

    test "handles comments" do
      input = """
      % Define users
      user("alice", admin).
      % Define permissions
      permission(admin, read).
      """

      {:ok, result} = Parser.parse(input)

      assert length(result) == 2
    end
  end

  describe "parse_file/1" do
    test "parses a file" do
      # Create a temp file
      path = Path.join(System.tmp_dir!(), "test_#{:erlang.unique_integer()}.dl")
      File.write!(path, ~s|user("alice", admin).|)

      {:ok, result} = Parser.parse_file(path)
      assert {:fact, {:user, ["alice", :admin]}} in result

      File.rm!(path)
    end
  end

  describe "parse/1 with aggregation" do
    test "parses rule with count aggregation" do
      input = "dept_count(Dept, N) :- employee(_, Dept), N = count(Dept)."
      {:ok, [result]} = Parser.parse(input)

      assert {:rule, rule} = result
      assert rule.head == {:dept_count, [:Dept, :N]}
      assert rule.body == [{:employee, [:_, :Dept]}]
      assert [{:count, :N, :_, [:Dept]}] = rule.aggregations
    end

    test "parses rule with sum aggregation" do
      input = "total(Person, T) :- sale(Person, Amount), T = sum(Amount)."
      {:ok, [result]} = Parser.parse(input)

      assert {:rule, rule} = result
      assert [{:sum, :T, :Amount, _group_vars}] = rule.aggregations
    end
  end
end

defmodule Datalox.Parser.GuardsParsingTest do
  use ExUnit.Case, async: true

  alias Datalox.Parser.Parser
  alias Datalox.Evaluator
  alias Datalox.Storage.ETS

  describe "comparison guard parsing" do
    test "simple greater-than" do
      {:ok, [{:rule, rule}]} = Parser.parse("high(X) :- val(X), X > 5.")
      assert rule.head == {:high, [:X]}
      assert rule.body == [{:val, [:X]}]
      assert rule.guards == [{:>, :X, 5}]
    end

    test "simple less-than" do
      {:ok, [{:rule, rule}]} = Parser.parse("low(X) :- val(X), X < 10.")
      assert rule.guards == [{:<, :X, 10}]
    end

    test "greater-than-or-equal" do
      {:ok, [{:rule, rule}]} = Parser.parse("r(X) :- v(X), X >= 5.")
      assert rule.guards == [{:>=, :X, 5}]
    end

    test "less-than-or-equal" do
      {:ok, [{:rule, rule}]} = Parser.parse("r(X) :- v(X), X <= 5.")
      assert rule.guards == [{:<=, :X, 5}]
    end

    test "not-equal" do
      {:ok, [{:rule, rule}]} = Parser.parse("diff(X, Y) :- pair(X, Y), X != Y.")
      assert rule.guards == [{:!=, :X, :Y}]
    end

    test "multiple guards in one rule" do
      {:ok, [{:rule, rule}]} = Parser.parse("mid(X, V) :- score(X, V), V > 30, V < 80.")
      assert rule.guards == [{:>, :V, 30}, {:<, :V, 80}]
    end
  end

  describe "arithmetic assignment parsing" do
    test "simple addition assignment" do
      {:ok, [{:rule, rule}]} = Parser.parse("total(X, T) :- a(X, A), b(X, B), T = A + B.")
      assert rule.guards == [{:=, :T, {:+, :A, :B}}]
    end

    test "subtraction assignment" do
      {:ok, [{:rule, rule}]} =
        Parser.parse("free(N, F) :- total(N, T), used(N, U), F = T - U.")

      assert rule.guards == [{:=, :F, {:-, :T, :U}}]
    end

    test "multiplication assignment" do
      {:ok, [{:rule, rule}]} =
        Parser.parse("with_tax(Item, Total) :- price(Item, P), Total = P * 1.1.")

      assert rule.guards == [{:=, :Total, {:*, :P, 1.1}}]
    end

    test "division assignment" do
      {:ok, [{:rule, rule}]} = Parser.parse("half(X, H) :- val(X, V), H = V / 2.")
      assert rule.guards == [{:=, :H, {:/, :V, 2}}]
    end
  end

  describe "arithmetic in comparisons" do
    test "subtraction in less-than comparison" do
      {:ok, [{:rule, rule}]} =
        Parser.parse("fast(X) :- time(X, T), start(X, S), T - S < 100.")

      assert rule.guards == [{:<, {:-, :T, :S}, 100}]
    end

    test "addition in greater-than comparison" do
      {:ok, [{:rule, rule}]} = Parser.parse("r(X) :- a(X, A), b(X, B), A + B > 10.")
      assert rule.guards == [{:>, {:+, :A, :B}, 10}]
    end
  end

  describe "operator precedence" do
    test "multiplication before addition" do
      {:ok, [{:rule, rule}]} = Parser.parse("r(X) :- v(X, A, B, C), A + B * C > 10.")
      assert rule.guards == [{:>, {:+, :A, {:*, :B, :C}}, 10}]
    end

    test "division before subtraction" do
      {:ok, [{:rule, rule}]} = Parser.parse("r(X) :- v(X, A, B), A - B / 2 > 0.")
      assert rule.guards == [{:>, {:-, :A, {:/, :B, 2}}, 0}]
    end

    test "parenthesized expression overrides precedence" do
      {:ok, [{:rule, rule}]} = Parser.parse("r(X) :- v(X, A, B, C), (A + B) * C > 10.")
      assert rule.guards == [{:>, {:*, {:+, :A, :B}, :C}, 10}]
    end
  end

  describe "negative numbers" do
    test "negative number in comparison" do
      {:ok, [{:rule, rule}]} = Parser.parse("cold(X) :- temp(X, T), T < -10.")
      assert rule.guards == [{:<, :T, -10}]
    end

    test "negative number in fact still works" do
      {:ok, [{:fact, fact}]} = Parser.parse("temp(cold, -5).")
      assert fact == {:temp, [:cold, -5]}
    end
  end

  describe "float support" do
    test "float in comparison" do
      {:ok, [{:rule, rule}]} = Parser.parse("r(X) :- v(X, V), V > 3.14.")
      assert rule.guards == [{:>, :V, 3.14}]
    end

    test "float in arithmetic assignment" do
      {:ok, [{:rule, rule}]} =
        Parser.parse("with_tax(I, T) :- price(I, P), T = P * 1.1.")

      assert rule.guards == [{:=, :T, {:*, :P, 1.1}}]
    end

    test "float in fact" do
      {:ok, [{:fact, fact}]} = Parser.parse("rate(usd, 1.25).")
      assert fact == {:rate, [:usd, 1.25]}
    end
  end

  describe "mixed guards and body goals" do
    test "assignment and comparison together" do
      {:ok, [{:rule, rule}]} =
        Parser.parse("with_tax(Item, Total) :- price(Item, P), Total = P * 1.1, Total > 5.")

      assert rule.body == [{:price, [:Item, :P]}]
      assert rule.guards == [{:=, :Total, {:*, :P, 1.1}}, {:>, :Total, 5}]
    end

    test "guards with negation" do
      {:ok, [{:rule, rule}]} =
        Parser.parse("r(X, V) :- val(X, V), V > 0, not banned(X).")

      assert rule.body == [{:val, [:X, :V]}]
      assert rule.negations == [{:banned, [:X]}]
      assert rule.guards == [{:>, :V, 0}]
    end

    test "multiple body goals with multiple guards" do
      input = """
      free(Node, CpuFree, MemFree) :-
        total(Node, CpuTotal, MemTotal),
        used(Node, CpuUsed, MemUsed),
        CpuFree = CpuTotal - CpuUsed,
        MemFree = MemTotal - MemUsed.
      """

      {:ok, [{:rule, rule}]} = Parser.parse(input)

      assert rule.body == [
               {:total, [:Node, :CpuTotal, :MemTotal]},
               {:used, [:Node, :CpuUsed, :MemUsed]}
             ]

      assert rule.guards == [
               {:=, :CpuFree, {:-, :CpuTotal, :CpuUsed}},
               {:=, :MemFree, {:-, :MemTotal, :MemUsed}}
             ]
    end
  end

  describe "end-to-end: parsed guards evaluate correctly" do
    setup do
      {:ok, storage} = ETS.init(name: :"guards_parse_e2e_#{:erlang.unique_integer()}")
      on_exit(fn -> ETS.terminate(storage) end)
      {:ok, storage: storage}
    end

    test "parsed comparison rule filters facts", %{storage: storage} do
      {:ok, storage} = ETS.insert(storage, :score, ["alice", 90])
      {:ok, storage} = ETS.insert(storage, :score, ["bob", 40])
      {:ok, storage} = ETS.insert(storage, :score, ["carol", 70])

      {:ok, [{:rule, rule}]} =
        Parser.parse("high_score(Name, S) :- score(Name, S), S > 50.")

      {:ok, derived, _} = Evaluator.evaluate([rule], storage, ETS)
      assert {:high_score, ["alice", 90]} in derived
      assert {:high_score, ["carol", 70]} in derived
      refute Enum.any?(derived, fn {_, [n, _]} -> n == "bob" end)
    end

    test "parsed arithmetic assignment produces correct values", %{storage: storage} do
      {:ok, storage} = ETS.insert(storage, :price, ["widget", 100])

      {:ok, [{:rule, rule}]} =
        Parser.parse("with_tax(Item, Total) :- price(Item, P), Total = P * 1.1.")

      {:ok, derived, _} = Evaluator.evaluate([rule], storage, ETS)
      assert {:with_tax, ["widget", 110.00000000000001]} in derived
    end

    test "parsed multiple guards filter correctly", %{storage: storage} do
      {:ok, storage} = ETS.insert(storage, :score, ["alice", 90])
      {:ok, storage} = ETS.insert(storage, :score, ["bob", 40])
      {:ok, storage} = ETS.insert(storage, :score, ["carol", 70])

      {:ok, [{:rule, rule}]} =
        Parser.parse("mid_score(Name, S) :- score(Name, S), S > 50, S < 80.")

      {:ok, derived, _} = Evaluator.evaluate([rule], storage, ETS)
      assert {:mid_score, ["carol", 70]} in derived
      refute Enum.any?(derived, fn {_, [n, _]} -> n == "alice" end)
      refute Enum.any?(derived, fn {_, [n, _]} -> n == "bob" end)
    end

    test "parsed not-equal guard works", %{storage: storage} do
      {:ok, storage} = ETS.insert(storage, :pair, ["a", "b"])
      {:ok, storage} = ETS.insert(storage, :pair, ["a", "a"])

      {:ok, [{:rule, rule}]} =
        Parser.parse("diff_pair(X, Y) :- pair(X, Y), X != Y.")

      {:ok, derived, _} = Evaluator.evaluate([rule], storage, ETS)
      assert {:diff_pair, ["a", "b"]} in derived
      refute {:diff_pair, ["a", "a"]} in derived
    end

    test "parsed arithmetic subtraction in comparison", %{storage: storage} do
      {:ok, storage} = ETS.insert(storage, :time_entry, ["task1", 100])
      {:ok, storage} = ETS.insert(storage, :start_entry, ["task1", 10])
      {:ok, storage} = ETS.insert(storage, :time_entry, ["task2", 200])
      {:ok, storage} = ETS.insert(storage, :start_entry, ["task2", 50])

      {:ok, [{:rule, rule}]} =
        Parser.parse("fast(X) :- time_entry(X, T), start_entry(X, S), T - S < 100.")

      {:ok, derived, _} = Evaluator.evaluate([rule], storage, ETS)
      assert {:fast, ["task1"]} in derived
      refute Enum.any?(derived, fn {_, [n]} -> n == "task2" end)
    end

    test "parsed assignment + comparison combined", %{storage: storage} do
      {:ok, storage} = ETS.insert(storage, :price, ["cheap", 3])
      {:ok, storage} = ETS.insert(storage, :price, ["mid", 10])

      {:ok, [{:rule, rule}]} =
        Parser.parse("with_tax(Item, Total) :- price(Item, P), Total = P * 1.1, Total > 5.")

      {:ok, derived, _} = Evaluator.evaluate([rule], storage, ETS)
      assert {:with_tax, ["mid", 11.0]} in derived
      refute Enum.any?(derived, fn {_, [n, _]} -> n == "cheap" end)
    end
  end
end

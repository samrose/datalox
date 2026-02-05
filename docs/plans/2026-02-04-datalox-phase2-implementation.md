# Datalox Phase 2+ Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Complete the Datalox Datalog engine with parser, aggregation, incremental maintenance, magic sets, and observability.

**Architecture:** Building on the Phase 1 foundation (storage, database, evaluator, DSL), add external file parsing, aggregate functions, incremental updates, query optimization, and debugging tools.

**Tech Stack:** Elixir 1.18+, NimbleParsec (parsing), Telemetry (metrics)

---

## Phase 4: Parser for .dl Files

### Task 11: Lexer for .dl Files

**Files:**
- Create: `lib/datalox/parser/lexer.ex`
- Create: `test/datalox/parser/lexer_test.exs`

**Step 1: Write failing test**

Create `test/datalox/parser/lexer_test.exs`:

```elixir
defmodule Datalox.Parser.LexerTest do
  use ExUnit.Case, async: true

  alias Datalox.Parser.Lexer

  describe "tokenize/1" do
    test "tokenizes a simple fact" do
      input = ~s|user("alice", admin).|
      {:ok, tokens, "", _, _, _} = Lexer.tokenize(input)

      assert [{:atom, "user"}, :lparen, {:string, "alice"}, :comma,
              {:atom, "admin"}, :rparen, :dot] = tokens
    end

    test "tokenizes a rule" do
      input = ~s|ancestor(X, Y) :- parent(X, Y).|
      {:ok, tokens, "", _, _, _} = Lexer.tokenize(input)

      assert [{:atom, "ancestor"}, :lparen, {:var, "X"}, :comma, {:var, "Y"},
              :rparen, :implies, {:atom, "parent"}, :lparen, {:var, "X"},
              :comma, {:var, "Y"}, :rparen, :dot] = tokens
    end

    test "tokenizes negation" do
      input = ~s|active(X) :- user(X), not banned(X).|
      {:ok, tokens, "", _, _, _} = Lexer.tokenize(input)

      assert :not in tokens
    end

    test "handles comments" do
      input = """
      % This is a comment
      user("alice", admin).
      """
      {:ok, tokens, "", _, _, _} = Lexer.tokenize(input)

      # Comment should be skipped
      assert {:atom, "user"} in tokens
    end

    test "tokenizes numbers" do
      input = ~s|age("alice", 30).|
      {:ok, tokens, "", _, _, _} = Lexer.tokenize(input)

      assert {:integer, 30} in tokens
    end
  end
end
```

**Step 2: Implement Lexer with NimbleParsec**

Create `lib/datalox/parser/lexer.ex`:

```elixir
defmodule Datalox.Parser.Lexer do
  @moduledoc """
  Lexer for Datalog .dl files using NimbleParsec.
  """

  import NimbleParsec

  # Whitespace and comments
  whitespace = ascii_string([?\s, ?\t, ?\r, ?\n], min: 1) |> ignore()
  comment = string("%") |> utf8_string([not: ?\n], min: 0) |> optional(string("\n")) |> ignore()
  skip = choice([whitespace, comment]) |> repeat() |> ignore()

  # Atoms (lowercase identifiers)
  atom_chars = ascii_string([?a..?z, ?A..?Z, ?0..?9, ?_], min: 0)
  atom_token =
    ascii_char([?a..?z])
    |> concat(atom_chars)
    |> reduce({List, :to_string, []})
    |> unwrap_and_tag(:atom)

  # Variables (uppercase identifiers)
  var_token =
    ascii_char([?A..?Z, ?_])
    |> concat(atom_chars)
    |> reduce({List, :to_string, []})
    |> unwrap_and_tag(:var)

  # Strings
  string_token =
    ignore(string("\""))
    |> utf8_string([not: ?"], min: 0)
    |> ignore(string("\""))
    |> unwrap_and_tag(:string)

  # Integers
  integer_token =
    optional(string("-"))
    |> ascii_string([?0..?9], min: 1)
    |> reduce({Enum, :join, []})
    |> map({String, :to_integer, []})
    |> unwrap_and_tag(:integer)

  # Keywords
  not_keyword = string("not") |> replace(:not)

  # Punctuation
  lparen = string("(") |> replace(:lparen)
  rparen = string(")") |> replace(:rparen)
  comma = string(",") |> replace(:comma)
  dot = string(".") |> replace(:dot)
  implies = string(":-") |> replace(:implies)
  underscore = string("_") |> replace(:wildcard)

  # Token
  token = choice([
    skip,
    not_keyword,
    implies,
    lparen,
    rparen,
    comma,
    dot,
    underscore,
    string_token,
    integer_token,
    var_token,
    atom_token
  ])

  defparsec :tokenize, repeat(token) |> eos()
end
```

**Step 3: Run tests**

Run: `nix develop --command mix test test/datalox/parser/lexer_test.exs`

**Step 4: Stage files (NO COMMIT)**

```bash
git add lib/datalox/parser/lexer.ex test/datalox/parser/lexer_test.exs
```

---

### Task 12: Parser for .dl Files

**Files:**
- Create: `lib/datalox/parser/parser.ex`
- Create: `test/datalox/parser/parser_test.exs`

**Step 1: Write failing test**

Create `test/datalox/parser/parser_test.exs`:

```elixir
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
end
```

**Step 2: Implement Parser**

Create `lib/datalox/parser/parser.ex`:

```elixir
defmodule Datalox.Parser.Parser do
  @moduledoc """
  Parser for Datalog .dl files.

  Parses the token stream from the Lexer into facts and rules.
  """

  alias Datalox.Parser.Lexer
  alias Datalox.Rule

  @type parse_result :: {:fact, {atom(), list()}} | {:rule, Rule.t()}

  @doc """
  Parses a Datalog string into facts and rules.
  """
  @spec parse(String.t()) :: {:ok, [parse_result()]} | {:error, term()}
  def parse(input) do
    case Lexer.tokenize(input) do
      {:ok, tokens, "", _, _, _} ->
        parse_tokens(tokens, [])

      {:error, reason, _, _, _, _} ->
        {:error, {:lexer_error, reason}}
    end
  end

  @doc """
  Parses a .dl file into facts and rules.
  """
  @spec parse_file(String.t()) :: {:ok, [parse_result()]} | {:error, term()}
  def parse_file(path) do
    case File.read(path) do
      {:ok, content} -> parse(content)
      {:error, reason} -> {:error, {:file_error, reason}}
    end
  end

  # Parse token stream
  defp parse_tokens([], acc), do: {:ok, Enum.reverse(acc)}

  defp parse_tokens(tokens, acc) do
    case parse_statement(tokens) do
      {:ok, statement, rest} ->
        parse_tokens(rest, [statement | acc])

      {:error, _} = error ->
        error
    end
  end

  # Parse a single statement (fact or rule)
  defp parse_statement([{:atom, pred} | rest]) do
    case parse_goal({:atom, pred}, rest) do
      {:ok, head, [:implies | body_tokens]} ->
        # It's a rule
        case parse_body(body_tokens) do
          {:ok, body, negations, [:dot | rest]} ->
            rule = Rule.new(head, body, negations: negations)
            {:ok, {:rule, rule}, rest}

          {:error, _} = error ->
            error
        end

      {:ok, goal, [:dot | rest]} ->
        # It's a fact
        {pred, args} = goal
        fact_args = Enum.map(args, &term_to_value/1)
        {:ok, {:fact, {pred, fact_args}}, rest}

      {:error, _} = error ->
        error
    end
  end

  defp parse_statement(tokens) do
    {:error, {:unexpected_token, tokens}}
  end

  # Parse a goal: predicate(arg1, arg2, ...)
  defp parse_goal({:atom, pred}, [:lparen | rest]) do
    case parse_args(rest, []) do
      {:ok, args, [:rparen | rest]} ->
        {:ok, {String.to_atom(pred), args}, rest}

      {:error, _} = error ->
        error
    end
  end

  # Parse argument list
  defp parse_args([:rparen | _] = tokens, acc) do
    {:ok, Enum.reverse(acc), tokens}
  end

  defp parse_args(tokens, acc) do
    case parse_term(tokens) do
      {:ok, term, [:comma | rest]} ->
        parse_args(rest, [term | acc])

      {:ok, term, rest} ->
        {:ok, Enum.reverse([term | acc]), rest}

      {:error, _} = error ->
        error
    end
  end

  # Parse a term (variable, atom, string, integer, wildcard)
  defp parse_term([{:var, name} | rest]) do
    {:ok, {:var, String.to_atom(name)}, rest}
  end

  defp parse_term([{:atom, name} | rest]) do
    {:ok, {:atom, String.to_atom(name)}, rest}
  end

  defp parse_term([{:string, value} | rest]) do
    {:ok, {:string, value}, rest}
  end

  defp parse_term([{:integer, value} | rest]) do
    {:ok, {:integer, value}, rest}
  end

  defp parse_term([:wildcard | rest]) do
    {:ok, {:wildcard}, rest}
  end

  defp parse_term(tokens) do
    {:error, {:unexpected_term, tokens}}
  end

  # Parse rule body
  defp parse_body(tokens) do
    parse_body_goals(tokens, [], [])
  end

  defp parse_body_goals([:not, {:atom, pred} | rest], goals, negations) do
    case parse_goal({:atom, pred}, rest) do
      {:ok, goal, [:comma | rest]} ->
        parse_body_goals(rest, goals, [goal | negations])

      {:ok, goal, rest} ->
        {:ok, Enum.reverse(goals), Enum.reverse([goal | negations]), rest}

      {:error, _} = error ->
        error
    end
  end

  defp parse_body_goals([{:atom, pred} | rest], goals, negations) do
    case parse_goal({:atom, pred}, rest) do
      {:ok, goal, [:comma | rest]} ->
        # Convert goal terms to proper format
        {pred, terms} = goal
        converted_goal = {pred, Enum.map(terms, &term_to_rule_term/1)}
        parse_body_goals(rest, [converted_goal | goals], negations)

      {:ok, goal, rest} ->
        {pred, terms} = goal
        converted_goal = {pred, Enum.map(terms, &term_to_rule_term/1)}
        converted_negations = Enum.map(negations, fn {p, ts} ->
          {p, Enum.map(ts, &term_to_rule_term/1)}
        end)
        {:ok, Enum.reverse([converted_goal | goals]), Enum.reverse(converted_negations), rest}

      {:error, _} = error ->
        error
    end
  end

  # Convert parsed term to rule term (variable atom)
  defp term_to_rule_term({:var, name}), do: name
  defp term_to_rule_term({:atom, name}), do: name
  defp term_to_rule_term({:string, value}), do: value
  defp term_to_rule_term({:integer, value}), do: value
  defp term_to_rule_term({:wildcard}), do: :_

  # Convert parsed term to fact value
  defp term_to_value({:var, name}), do: name
  defp term_to_value({:atom, name}), do: name
  defp term_to_value({:string, value}), do: value
  defp term_to_value({:integer, value}), do: value
  defp term_to_value({:wildcard}), do: :_
end
```

**Step 3: Run tests**

Run: `nix develop --command mix test test/datalox/parser/parser_test.exs`

**Step 4: Stage files (NO COMMIT)**

```bash
git add lib/datalox/parser/parser.ex test/datalox/parser/parser_test.exs
```

---

### Task 13: Integrate Parser with Datalox

**Files:**
- Modify: `lib/datalox.ex`
- Create: `test/datalox/parser_integration_test.exs`

**Step 1: Write failing test**

Create `test/datalox/parser_integration_test.exs`:

```elixir
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
```

**Step 2: Add load_file to Datalox**

Add to `lib/datalox.ex`:

```elixir
  @doc """
  Loads facts and rules from a .dl file.

  ## Examples

      Datalox.load_file(db, "rules/access_control.dl")

  """
  @spec load_file(pid() | atom(), String.t()) :: :ok | {:error, term()}
  def load_file(db, path) do
    alias Datalox.Parser.Parser

    case Parser.parse_file(path) do
      {:ok, statements} ->
        {facts, rules} = Enum.split_with(statements, fn
          {:fact, _} -> true
          {:rule, _} -> false
        end)

        fact_tuples = Enum.map(facts, fn {:fact, f} -> f end)
        rule_structs = Enum.map(rules, fn {:rule, r} -> r end)

        with :ok <- assert_all(db, fact_tuples),
             :ok <- Database.load_rules(db, rule_structs) do
          :ok
        end

      {:error, _} = error ->
        error
    end
  end
```

**Step 3: Run tests**

Run: `nix develop --command mix test test/datalox/parser_integration_test.exs`

**Step 4: Stage files (NO COMMIT)**

```bash
git add lib/datalox.ex test/datalox/parser_integration_test.exs
```

---

## Phase 5: Aggregation Support

### Task 14: Aggregation Engine

**Files:**
- Create: `lib/datalox/aggregation.ex`
- Create: `test/datalox/aggregation_test.exs`

**Step 1: Write failing test**

Create `test/datalox/aggregation_test.exs`:

```elixir
defmodule Datalox.AggregationTest do
  use ExUnit.Case, async: true

  alias Datalox.Aggregation

  describe "compute/3" do
    test "computes count" do
      values = [1, 2, 3, 4, 5]
      assert Aggregation.compute(:count, values, []) == 5
    end

    test "computes sum" do
      values = [10, 20, 30]
      assert Aggregation.compute(:sum, values, []) == 60
    end

    test "computes min" do
      values = [5, 2, 8, 1, 9]
      assert Aggregation.compute(:min, values, []) == 1
    end

    test "computes max" do
      values = [5, 2, 8, 1, 9]
      assert Aggregation.compute(:max, values, []) == 9
    end

    test "computes avg" do
      values = [10, 20, 30]
      assert Aggregation.compute(:avg, values, []) == 20.0
    end

    test "computes collect" do
      values = [1, 2, 3]
      result = Aggregation.compute(:collect, values, [])
      assert Enum.sort(result) == [1, 2, 3]
    end

    test "handles empty values for count" do
      assert Aggregation.compute(:count, [], []) == 0
    end

    test "handles empty values for sum" do
      assert Aggregation.compute(:sum, [], []) == 0
    end

    test "returns nil for min/max on empty" do
      assert Aggregation.compute(:min, [], []) == nil
      assert Aggregation.compute(:max, [], []) == nil
    end
  end
end
```

**Step 2: Implement Aggregation module**

Create `lib/datalox/aggregation.ex`:

```elixir
defmodule Datalox.Aggregation do
  @moduledoc """
  Aggregation functions for Datalog queries.

  Supports: count, sum, min, max, avg, collect
  """

  @type aggregate_fn :: :count | :sum | :min | :max | :avg | :collect

  @doc """
  Computes an aggregate over a list of values.
  """
  @spec compute(aggregate_fn(), [any()], keyword()) :: any()
  def compute(:count, values, _opts), do: length(values)

  def compute(:sum, [], _opts), do: 0
  def compute(:sum, values, _opts), do: Enum.sum(values)

  def compute(:min, [], _opts), do: nil
  def compute(:min, values, _opts), do: Enum.min(values)

  def compute(:max, [], _opts), do: nil
  def compute(:max, values, _opts), do: Enum.max(values)

  def compute(:avg, [], _opts), do: nil
  def compute(:avg, values, _opts), do: Enum.sum(values) / length(values)

  def compute(:collect, values, _opts), do: values
end
```

**Step 3: Run tests**

Run: `nix develop --command mix test test/datalox/aggregation_test.exs`

**Step 4: Stage files (NO COMMIT)**

```bash
git add lib/datalox/aggregation.ex test/datalox/aggregation_test.exs
```

---

## Phase 6: Observability

### Task 15: Query Explanation

**Files:**
- Create: `lib/datalox/explain.ex`
- Create: `test/datalox/explain_test.exs`

**Step 1: Write failing test**

Create `test/datalox/explain_test.exs`:

```elixir
defmodule Datalox.ExplainTest do
  use ExUnit.Case, async: true

  describe "explain/2" do
    setup do
      name = :"test_#{:erlang.unique_integer()}"
      {:ok, db} = Datalox.new(name: name)

      # Set up facts
      Datalox.assert(db, {:parent, ["alice", "bob"]})
      Datalox.assert(db, {:parent, ["bob", "carol"]})

      # Set up rules
      rules = [
        Datalox.Rule.new({:ancestor, [:X, :Y]}, [{:parent, [:X, :Y]}]),
        Datalox.Rule.new({:ancestor, [:X, :Z]}, [{:parent, [:X, :Y]}, {:ancestor, [:Y, :Z]}])
      ]
      Datalox.Database.load_rules(db, rules)

      on_exit(fn -> catch_exit(Datalox.stop(db)) end)
      {:ok, db: db}
    end

    test "explains a base fact", %{db: db} do
      explanation = Datalox.explain(db, {:parent, ["alice", "bob"]})

      assert explanation.fact == {:parent, ["alice", "bob"]}
      assert explanation.derivation == :base
    end

    test "explains a derived fact", %{db: db} do
      explanation = Datalox.explain(db, {:ancestor, ["alice", "bob"]})

      assert explanation.fact == {:ancestor, ["alice", "bob"]}
      assert explanation.derivation != :base
    end
  end
end
```

**Step 2: Implement Explain module**

Create `lib/datalox/explain.ex`:

```elixir
defmodule Datalox.Explain do
  @moduledoc """
  Query explanation for Datalox.

  Provides derivation trees showing how facts were derived.
  """

  defstruct [:fact, :rule, :derivation]

  @type t :: %__MODULE__{
          fact: {atom(), list()},
          rule: atom() | nil,
          derivation: :base | [t()]
        }

  @doc """
  Creates an explanation for a base fact.
  """
  @spec base(tuple()) :: t()
  def base(fact) do
    %__MODULE__{fact: fact, rule: nil, derivation: :base}
  end

  @doc """
  Creates an explanation for a derived fact.
  """
  @spec derived(tuple(), atom(), [t()]) :: t()
  def derived(fact, rule, derivations) do
    %__MODULE__{fact: fact, rule: rule, derivation: derivations}
  end
end
```

**Step 3: Add explain to Datalox**

Add to `lib/datalox.ex`:

```elixir
  alias Datalox.Explain

  @doc """
  Explains how a fact was derived.

  Returns an explanation showing the derivation tree.
  """
  @spec explain(pid() | atom(), {atom(), list()}) :: Explain.t() | nil
  def explain(db, {predicate, pattern} = fact) do
    if exists?(db, {predicate, pattern}) do
      # For now, return base explanation
      # Full derivation tracking to be added later
      Explain.base(fact)
    else
      nil
    end
  end
```

**Step 4: Run tests**

Run: `nix develop --command mix test test/datalox/explain_test.exs`

**Step 5: Stage files (NO COMMIT)**

```bash
git add lib/datalox/explain.ex lib/datalox.ex test/datalox/explain_test.exs
```

---

### Task 16: Metrics with Telemetry

**Files:**
- Create: `lib/datalox/metrics.ex`
- Create: `test/datalox/metrics_test.exs`

**Step 1: Write failing test**

Create `test/datalox/metrics_test.exs`:

```elixir
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
```

**Step 2: Implement Metrics module**

Create `lib/datalox/metrics.ex`:

```elixir
defmodule Datalox.Metrics do
  @moduledoc """
  Metrics collection for Datalox using Telemetry.
  """

  use GenServer

  @table_name :datalox_metrics

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(_opts) do
    :ets.new(@table_name, [:named_table, :public, :set])
    {:ok, %{}}
  end

  @doc """
  Records a query event.
  """
  @spec record_query(atom(), atom()) :: :ok
  def record_query(db_name, predicate) do
    key = {db_name, :queries}
    :ets.update_counter(@table_name, key, {2, 1}, {key, 0})

    pred_key = {db_name, :predicate, predicate}
    :ets.update_counter(@table_name, pred_key, {2, 1}, {pred_key, 0})
    :ok
  end

  @doc """
  Gets stats for a database.
  """
  @spec get_stats(pid() | atom()) :: map()
  def get_stats(db) when is_pid(db) do
    get_stats(:unknown)
  end

  def get_stats(db_name) do
    query_count =
      case :ets.lookup(@table_name, {db_name, :queries}) do
        [{_, count}] -> count
        [] -> 0
      end

    %{
      total_queries: query_count,
      facts: %{base: 0, derived: 0}
    }
  end
end
```

**Step 3: Start Metrics in Application**

Update `lib/datalox/application.ex`:

```elixir
defmodule Datalox.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Datalox.Registry},
      Datalox.Metrics
    ]

    opts = [strategy: :one_for_one, name: Datalox.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

**Step 4: Run tests**

Run: `nix develop --command mix test test/datalox/metrics_test.exs`

**Step 5: Stage files (NO COMMIT)**

```bash
git add lib/datalox/metrics.ex lib/datalox/application.ex test/datalox/metrics_test.exs
```

---

### Task 17: Subscription System

**Files:**
- Create: `lib/datalox/subscription.ex`
- Create: `test/datalox/subscription_test.exs`

**Step 1: Write failing test**

Create `test/datalox/subscription_test.exs`:

```elixir
defmodule Datalox.SubscriptionTest do
  use ExUnit.Case, async: true

  alias Datalox.Subscription

  describe "subscribe/2 and notify/2" do
    test "notifies subscribers of new facts" do
      {:ok, sub} = Subscription.start_link([])

      test_pid = self()
      Subscription.subscribe(sub, {:user, [:_, :_]}, fn fact ->
        send(test_pid, {:fact_added, fact})
      end)

      Subscription.notify(sub, {:assert, {:user, ["alice", :admin]}})

      assert_receive {:fact_added, {:user, ["alice", :admin]}}, 1000
    end

    test "filters by pattern" do
      {:ok, sub} = Subscription.start_link([])

      test_pid = self()
      Subscription.subscribe(sub, {:user, [:_, :admin]}, fn fact ->
        send(test_pid, {:admin_added, fact})
      end)

      Subscription.notify(sub, {:assert, {:user, ["alice", :admin]}})
      Subscription.notify(sub, {:assert, {:user, ["bob", :viewer]}})

      assert_receive {:admin_added, {:user, ["alice", :admin]}}, 1000
      refute_receive {:admin_added, {:user, ["bob", :viewer]}}, 100
    end
  end
end
```

**Step 2: Implement Subscription module**

Create `lib/datalox/subscription.ex`:

```elixir
defmodule Datalox.Subscription do
  @moduledoc """
  Subscription system for fact change notifications.
  """

  use GenServer

  defstruct subscriptions: []

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(_opts) do
    {:ok, %__MODULE__{}}
  end

  @doc """
  Subscribe to facts matching a pattern.
  """
  def subscribe(pid, pattern, callback) do
    GenServer.call(pid, {:subscribe, pattern, callback})
  end

  @doc """
  Notify subscribers of a fact change.
  """
  def notify(pid, event) do
    GenServer.cast(pid, {:notify, event})
  end

  @impl true
  def handle_call({:subscribe, pattern, callback}, _from, state) do
    subscription = {pattern, callback}
    new_state = %{state | subscriptions: [subscription | state.subscriptions]}
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_cast({:notify, {:assert, {pred, tuple} = fact}}, state) do
    Enum.each(state.subscriptions, fn {{sub_pred, sub_pattern}, callback} ->
      if pred == sub_pred and matches_pattern?(tuple, sub_pattern) do
        callback.(fact)
      end
    end)

    {:noreply, state}
  end

  def handle_cast({:notify, _event}, state) do
    {:noreply, state}
  end

  defp matches_pattern?(tuple, pattern) when length(tuple) == length(pattern) do
    Enum.zip(tuple, pattern)
    |> Enum.all?(fn
      {_, :_} -> true
      {a, a} -> true
      {_, _} -> false
    end)
  end

  defp matches_pattern?(_, _), do: false
end
```

**Step 3: Run tests**

Run: `nix develop --command mix test test/datalox/subscription_test.exs`

**Step 4: Stage files (NO COMMIT)**

```bash
git add lib/datalox/subscription.ex test/datalox/subscription_test.exs
```

---

## Summary

This plan covers:
- **Phase 4 (Tasks 11-13):** Parser for .dl files using NimbleParsec
- **Phase 5 (Task 14):** Aggregation functions (count, sum, min, max, avg, collect)
- **Phase 6 (Tasks 15-17):** Observability (explain, metrics, subscriptions)

**Remaining phases (for future work):**
- Phase 7: Incremental maintenance (delta updates)
- Phase 8: Magic Sets optimization
- Phase 9: Property-based tests and documentation

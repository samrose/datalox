# Datalox Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a complete Datalog rule engine in Elixir with stratified negation, aggregation, incremental maintenance, and full observability.

**Architecture:** Three-layer design (Interface → Core Engine → Storage) with GenServer-based database instances. Incremental semi-naive evaluation with dependency tracking for efficient updates. Magic Sets transformation for query optimization.

**Tech Stack:** Elixir 1.18+, Erlang/OTP 27+, NimbleParsec (parsing), Telemetry (metrics), StreamData (property tests)

---

## Phase 1: Project Foundation

### Task 1: Initialize Mix Project

**Files:**
- Create: `mix.exs`
- Create: `lib/datalox.ex`
- Create: `lib/datalox/application.ex`
- Create: `test/test_helper.exs`
- Create: `test/datalox_test.exs`

**Step 1: Create Mix project structure**

Run: `nix develop --command mix new datalox --app datalox --module Datalox`

Note: Since we're in the datalox directory already, we need to create in a temp location and move files.

Run:
```bash
cd /tmp && mix new datalox --app datalox --module Datalox && cp -r datalox/* /Users/samrose/datalox/ && rm -rf /tmp/datalox
```

**Step 2: Update mix.exs with dependencies**

Replace `mix.exs` content:

```elixir
defmodule Datalox.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/samrose/datalox"

  def project do
    [
      app: :datalox,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      package: package(),
      name: "Datalox",
      description: "A Datalog implementation in Elixir for rule engines",
      source_url: @source_url,
      dialyzer: [
        plt_add_apps: [:mix, :ex_unit],
        plt_file: {:no_warn, "priv/plts/dialyzer.plt"}
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Datalox.Application, []}
    ]
  end

  defp deps do
    [
      {:nimble_parsec, "~> 1.4"},
      {:telemetry, "~> 1.2"},
      {:stream_data, "~> 1.0", only: [:test, :dev]},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false}
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "LICENSE"],
      source_url: @source_url
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
    ]
  end
end
```

**Step 3: Create application supervisor**

Create `lib/datalox/application.ex`:

```elixir
defmodule Datalox.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Datalox.Registry}
    ]

    opts = [strategy: :one_for_one, name: Datalox.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

**Step 4: Create main module stub**

Create `lib/datalox.ex`:

```elixir
defmodule Datalox do
  @moduledoc """
  Datalox - A Datalog implementation in Elixir.

  Datalox provides a rule engine based on Datalog with support for:
  - Stratified negation
  - Aggregation (count, sum, min, max, avg)
  - Incremental maintenance
  - Magic Sets query optimization
  - Pluggable storage backends

  ## Quick Start

      # Create a database
      {:ok, db} = Datalox.new(name: :my_db)

      # Assert facts
      Datalox.assert(db, {:user, ["alice", :admin]})
      Datalox.assert(db, {:user, ["bob", :viewer]})

      # Query
      Datalox.query(db, {:user, [:_, :admin]})
      #=> [{:user, ["alice", :admin]}]

  """

  @doc """
  Creates a new Datalox database.

  ## Options

    * `:name` - Required. The name to register the database under.
    * `:storage` - Storage backend module. Defaults to `Datalox.Storage.ETS`.

  ## Examples

      {:ok, db} = Datalox.new(name: :my_db)

  """
  @spec new(keyword()) :: {:ok, pid()} | {:error, term()}
  def new(_opts) do
    {:error, :not_implemented}
  end
end
```

**Step 5: Run tests to verify setup**

Run: `nix develop --command bash -c "mix deps.get && mix test"`

Expected: Tests pass (default test should pass)

**Step 6: Commit**

```bash
git add -A
git commit -m "feat: initialize Mix project with dependencies"
```

---

### Task 2: Error Types

**Files:**
- Create: `lib/datalox/errors.ex`
- Create: `test/datalox/errors_test.exs`

**Step 1: Write failing test**

Create `test/datalox/errors_test.exs`:

```elixir
defmodule Datalox.ErrorsTest do
  use ExUnit.Case, async: true

  alias Datalox.Errors.{
    StratificationError,
    UndefinedPredicateError,
    ArityError,
    SafetyError,
    TypeError
  }

  describe "StratificationError" do
    test "formats message with cycle path" do
      error = %StratificationError{
        cycle: [:bad, :bar, :bad],
        location: "my_rules.ex:12"
      }

      message = Exception.message(error)
      assert message =~ "Circular negation detected"
      assert message =~ "bad -> bar -> bad"
      assert message =~ "my_rules.ex:12"
    end
  end

  describe "UndefinedPredicateError" do
    test "suggests similar predicates" do
      error = %UndefinedPredicateError{
        predicate: :usr,
        suggestions: [:user],
        location: "my_rules.ex:8"
      }

      message = Exception.message(error)
      assert message =~ "Unknown predicate 'usr'"
      assert message =~ "Did you mean: user"
    end
  end

  describe "ArityError" do
    test "shows expected vs actual arity" do
      error = %ArityError{
        predicate: :user,
        expected: 3,
        actual: 2,
        location: "my_rules.ex:15"
      }

      message = Exception.message(error)
      assert message =~ "expects 3 arguments, got 2"
    end
  end

  describe "SafetyError" do
    test "identifies unsafe variable" do
      error = %SafetyError{
        variable: :X,
        context: "not banned(X)",
        location: "my_rules.ex:20"
      }

      message = Exception.message(error)
      assert message =~ "Variable 'X' is unsafe"
      assert message =~ "not banned(X)"
    end
  end

  describe "TypeError" do
    test "shows type mismatch in aggregation" do
      error = %TypeError{
        operation: :sum,
        expected: :numeric,
        actual: "not_a_number",
        context: "purchase/3"
      }

      message = Exception.message(error)
      assert message =~ "Cannot sum non-numeric value"
    end
  end
end
```

**Step 2: Run test to verify it fails**

Run: `nix develop --command mix test test/datalox/errors_test.exs`

Expected: FAIL - modules not defined

**Step 3: Implement error types**

Create `lib/datalox/errors.ex`:

```elixir
defmodule Datalox.Errors do
  @moduledoc """
  Custom error types for Datalox with helpful diagnostic messages.
  """

  defmodule StratificationError do
    @moduledoc """
    Raised when rules contain circular negation that cannot be stratified.
    """
    defexception [:cycle, :location]

    @impl true
    def message(%{cycle: cycle, location: location}) do
      cycle_str = Enum.join(cycle, " -> ")

      """
      Circular negation detected

         #{cycle_str}

         Rules with negation cannot form cycles. Consider:
         - Restructuring rules to break the cycle
         - Using positive conditions instead

         at #{location}
      """
    end
  end

  defmodule UndefinedPredicateError do
    @moduledoc """
    Raised when a rule references an undefined predicate.
    """
    defexception [:predicate, :suggestions, :location]

    @impl true
    def message(%{predicate: predicate, suggestions: suggestions, location: location}) do
      suggestion_str =
        case suggestions do
          [] -> ""
          [s] -> "\n   Did you mean: #{s}?"
          list -> "\n   Did you mean one of: #{Enum.join(list, ", ")}?"
        end

      """
      Unknown predicate '#{predicate}' in rule#{suggestion_str}

         at #{location}
      """
    end
  end

  defmodule ArityError do
    @moduledoc """
    Raised when a predicate is used with the wrong number of arguments.
    """
    defexception [:predicate, :expected, :actual, :location]

    @impl true
    def message(%{predicate: predicate, expected: expected, actual: actual, location: location}) do
      """
      Predicate '#{predicate}' expects #{expected} arguments, got #{actual}

         at #{location}
      """
    end
  end

  defmodule SafetyError do
    @moduledoc """
    Raised when a variable only appears in negated goals.
    """
    defexception [:variable, :context, :location]

    @impl true
    def message(%{variable: variable, context: context, location: location}) do
      """
      Variable '#{variable}' is unsafe

         #{variable} only appears in negated goal: #{context}
         Variables must appear in at least one positive goal.

         at #{location}
      """
    end
  end

  defmodule TypeError do
    @moduledoc """
    Raised when a type mismatch occurs during evaluation.
    """
    defexception [:operation, :expected, :actual, :context]

    @impl true
    def message(%{operation: operation, expected: _expected, actual: actual, context: context}) do
      """
      Cannot #{operation} non-numeric value

         Got: #{inspect(actual)} in #{context}
      """
    end
  end
end
```

**Step 4: Run test to verify it passes**

Run: `nix develop --command mix test test/datalox/errors_test.exs`

Expected: PASS

**Step 5: Commit**

```bash
git add lib/datalox/errors.ex test/datalox/errors_test.exs
git commit -m "feat: add custom error types with helpful diagnostics"
```

---

### Task 3: Rule Data Structure

**Files:**
- Create: `lib/datalox/rule.ex`
- Create: `test/datalox/rule_test.exs`

**Step 1: Write failing test**

Create `test/datalox/rule_test.exs`:

```elixir
defmodule Datalox.RuleTest do
  use ExUnit.Case, async: true

  alias Datalox.Rule

  describe "new/2" do
    test "creates a simple rule" do
      rule = Rule.new(
        {:ancestor, [:X, :Z]},
        [{:parent, [:X, :Z]}]
      )

      assert rule.head == {:ancestor, [:X, :Z]}
      assert rule.body == [{:parent, [:X, :Z]}]
      assert rule.negations == []
      assert rule.aggregations == []
    end

    test "creates a rule with negation" do
      rule = Rule.new(
        {:inactive, [:User]},
        [{:user, [:User, :_, :_]}],
        negations: [{:login, [:User, :_]}]
      )

      assert rule.negations == [{:login, [:User, :_]}]
    end

    test "creates a rule with aggregation" do
      rule = Rule.new(
        {:high_spender, [:User]},
        [{:user, [:User, :_, :_]}],
        aggregations: [
          {:sum, :amount, {:purchase, [:User, :_, :amount]}, {:>, 1000}}
        ]
      )

      assert length(rule.aggregations) == 1
    end
  end

  describe "variables/1" do
    test "extracts all variables from rule" do
      rule = Rule.new(
        {:ancestor, [:X, :Z]},
        [{:parent, [:X, :Y]}, {:ancestor, [:Y, :Z]}]
      )

      vars = Rule.variables(rule)
      assert MapSet.equal?(vars, MapSet.new([:X, :Y, :Z]))
    end

    test "ignores underscore wildcards" do
      rule = Rule.new(
        {:active, [:User]},
        [{:user, [:User, :_, :_]}]
      )

      vars = Rule.variables(rule)
      assert MapSet.equal?(vars, MapSet.new([:User]))
    end
  end

  describe "head_variables/1" do
    test "returns variables in rule head" do
      rule = Rule.new(
        {:ancestor, [:X, :Z]},
        [{:parent, [:X, :Y]}, {:ancestor, [:Y, :Z]}]
      )

      vars = Rule.head_variables(rule)
      assert MapSet.equal?(vars, MapSet.new([:X, :Z]))
    end
  end

  describe "depends_on/1" do
    test "returns predicates the rule depends on" do
      rule = Rule.new(
        {:can_access, [:User, :Resource]},
        [{:user, [:User, :Role, :Dept]}, {:resource, [:Resource, :Dept]}],
        negations: [{:banned, [:User]}]
      )

      deps = Rule.depends_on(rule)
      assert MapSet.equal?(deps, MapSet.new([:user, :resource, :banned]))
    end
  end

  describe "is_recursive?/1" do
    test "detects recursive rules" do
      rule = Rule.new(
        {:ancestor, [:X, :Z]},
        [{:parent, [:X, :Y]}, {:ancestor, [:Y, :Z]}]
      )

      assert Rule.is_recursive?(rule)
    end

    test "detects non-recursive rules" do
      rule = Rule.new(
        {:can_access, [:User, :Resource]},
        [{:user, [:User, :Role]}, {:permission, [:Role, :Resource]}]
      )

      refute Rule.is_recursive?(rule)
    end
  end
end
```

**Step 2: Run test to verify it fails**

Run: `nix develop --command mix test test/datalox/rule_test.exs`

Expected: FAIL - Rule module not defined

**Step 3: Implement Rule module**

Create `lib/datalox/rule.ex`:

```elixir
defmodule Datalox.Rule do
  @moduledoc """
  Represents a Datalog rule.

  A rule consists of:
  - `head` - The derived predicate (consequent)
  - `body` - List of positive goals (antecedents)
  - `negations` - List of negated goals
  - `aggregations` - List of aggregate expressions
  - `guards` - List of comparison guards
  """

  @type variable :: atom()
  @type predicate :: atom()
  @type term :: variable() | any()
  @type goal :: {predicate(), [term()]}
  @type aggregation :: {:sum | :count | :min | :max | :avg, variable(), goal(), {atom(), any()}}

  @type t :: %__MODULE__{
          head: goal(),
          body: [goal()],
          negations: [goal()],
          aggregations: [aggregation()],
          guards: [any()]
        }

  @enforce_keys [:head, :body]
  defstruct head: nil,
            body: [],
            negations: [],
            aggregations: [],
            guards: []

  @doc """
  Creates a new rule.

  ## Examples

      iex> Rule.new({:ancestor, [:X, :Z]}, [{:parent, [:X, :Z]}])
      %Rule{head: {:ancestor, [:X, :Z]}, body: [{:parent, [:X, :Z]}], ...}

  """
  @spec new(goal(), [goal()], keyword()) :: t()
  def new(head, body, opts \\ []) do
    %__MODULE__{
      head: head,
      body: body,
      negations: Keyword.get(opts, :negations, []),
      aggregations: Keyword.get(opts, :aggregations, []),
      guards: Keyword.get(opts, :guards, [])
    }
  end

  @doc """
  Returns all variables in the rule (head, body, negations).
  """
  @spec variables(t()) :: MapSet.t(variable())
  def variables(%__MODULE__{} = rule) do
    head_vars = extract_variables(rule.head)
    body_vars = Enum.flat_map(rule.body, &extract_variables/1)
    neg_vars = Enum.flat_map(rule.negations, &extract_variables/1)

    MapSet.new(head_vars ++ body_vars ++ neg_vars)
  end

  @doc """
  Returns variables appearing in the rule head.
  """
  @spec head_variables(t()) :: MapSet.t(variable())
  def head_variables(%__MODULE__{head: head}) do
    head
    |> extract_variables()
    |> MapSet.new()
  end

  @doc """
  Returns the set of predicates this rule depends on.
  """
  @spec depends_on(t()) :: MapSet.t(predicate())
  def depends_on(%__MODULE__{body: body, negations: negations}) do
    body_preds = Enum.map(body, fn {pred, _} -> pred end)
    neg_preds = Enum.map(negations, fn {pred, _} -> pred end)

    MapSet.new(body_preds ++ neg_preds)
  end

  @doc """
  Returns true if the rule is recursive (head predicate appears in body).
  """
  @spec is_recursive?(t()) :: boolean()
  def is_recursive?(%__MODULE__{head: {head_pred, _}, body: body}) do
    Enum.any?(body, fn {pred, _} -> pred == head_pred end)
  end

  # Extract variables from a goal, filtering out wildcards (:_)
  defp extract_variables({_predicate, terms}) do
    terms
    |> Enum.filter(&is_variable?/1)
    |> Enum.reject(&(&1 == :_))
  end

  # A term is a variable if it's an uppercase atom
  defp is_variable?(term) when is_atom(term) do
    term
    |> Atom.to_string()
    |> String.first()
    |> String.match?(~r/^[A-Z_]$/)
  end

  defp is_variable?(_), do: false
end
```

**Step 4: Run test to verify it passes**

Run: `nix develop --command mix test test/datalox/rule_test.exs`

Expected: PASS

**Step 5: Commit**

```bash
git add lib/datalox/rule.ex test/datalox/rule_test.exs
git commit -m "feat: add Rule struct for representing Datalog rules"
```

---

### Task 4: Storage Behaviour and ETS Adapter

**Files:**
- Create: `lib/datalox/storage.ex`
- Create: `lib/datalox/storage/ets.ex`
- Create: `test/datalox/storage/ets_test.exs`

**Step 1: Write failing test**

Create `test/datalox/storage/ets_test.exs`:

```elixir
defmodule Datalox.Storage.ETSTest do
  use ExUnit.Case, async: true

  alias Datalox.Storage.ETS

  setup do
    {:ok, state} = ETS.init(name: :test_storage)
    on_exit(fn -> ETS.terminate(state) end)
    {:ok, state: state}
  end

  describe "insert/3 and lookup/3" do
    test "inserts and retrieves a fact", %{state: state} do
      {:ok, state} = ETS.insert(state, :user, ["alice", :admin, "engineering"])
      {:ok, results} = ETS.lookup(state, :user, [:_, :_, :_])

      assert results == [["alice", :admin, "engineering"]]
    end

    test "handles multiple facts for same predicate", %{state: state} do
      {:ok, state} = ETS.insert(state, :user, ["alice", :admin, "engineering"])
      {:ok, state} = ETS.insert(state, :user, ["bob", :viewer, "sales"])
      {:ok, results} = ETS.lookup(state, :user, [:_, :_, :_])

      assert length(results) == 2
      assert ["alice", :admin, "engineering"] in results
      assert ["bob", :viewer, "sales"] in results
    end

    test "filters by pattern", %{state: state} do
      {:ok, state} = ETS.insert(state, :user, ["alice", :admin, "engineering"])
      {:ok, state} = ETS.insert(state, :user, ["bob", :viewer, "sales"])
      {:ok, results} = ETS.lookup(state, :user, [:_, :admin, :_])

      assert results == [["alice", :admin, "engineering"]]
    end

    test "returns empty list for no matches", %{state: state} do
      {:ok, state} = ETS.insert(state, :user, ["alice", :admin, "engineering"])
      {:ok, results} = ETS.lookup(state, :user, [:_, :nobody, :_])

      assert results == []
    end
  end

  describe "delete/3" do
    test "removes a fact", %{state: state} do
      {:ok, state} = ETS.insert(state, :user, ["alice", :admin, "engineering"])
      {:ok, state} = ETS.delete(state, :user, ["alice", :admin, "engineering"])
      {:ok, results} = ETS.lookup(state, :user, [:_, :_, :_])

      assert results == []
    end

    test "only removes exact match", %{state: state} do
      {:ok, state} = ETS.insert(state, :user, ["alice", :admin, "engineering"])
      {:ok, state} = ETS.insert(state, :user, ["bob", :viewer, "sales"])
      {:ok, state} = ETS.delete(state, :user, ["alice", :admin, "engineering"])
      {:ok, results} = ETS.lookup(state, :user, [:_, :_, :_])

      assert results == [["bob", :viewer, "sales"]]
    end
  end

  describe "all/2" do
    test "returns all facts for a predicate", %{state: state} do
      {:ok, state} = ETS.insert(state, :user, ["alice", :admin, "engineering"])
      {:ok, state} = ETS.insert(state, :user, ["bob", :viewer, "sales"])
      {:ok, state} = ETS.insert(state, :role, [:admin, :write])
      {:ok, results} = ETS.all(state, :user)

      assert length(results) == 2
    end

    test "returns empty list for unknown predicate", %{state: state} do
      {:ok, results} = ETS.all(state, :unknown)
      assert results == []
    end
  end
end
```

**Step 2: Run test to verify it fails**

Run: `nix develop --command mix test test/datalox/storage/ets_test.exs`

Expected: FAIL - modules not defined

**Step 3: Implement Storage behaviour**

Create `lib/datalox/storage.ex`:

```elixir
defmodule Datalox.Storage do
  @moduledoc """
  Behaviour for Datalox storage backends.

  A storage backend is responsible for persisting and querying facts.
  The default implementation uses ETS tables.
  """

  @type state :: any()
  @type predicate :: atom()
  @type tuple :: [any()]
  @type pattern :: [any()]

  @doc """
  Initialize the storage backend.
  """
  @callback init(opts :: keyword()) :: {:ok, state()} | {:error, term()}

  @doc """
  Insert a fact into storage.
  """
  @callback insert(state(), predicate(), tuple()) :: {:ok, state()} | {:error, term()}

  @doc """
  Delete a fact from storage.
  """
  @callback delete(state(), predicate(), tuple()) :: {:ok, state()} | {:error, term()}

  @doc """
  Lookup facts matching a pattern. Use :_ for wildcards.
  """
  @callback lookup(state(), predicate(), pattern()) :: {:ok, [tuple()]} | {:error, term()}

  @doc """
  Return all facts for a predicate.
  """
  @callback all(state(), predicate()) :: {:ok, [tuple()]} | {:error, term()}

  @doc """
  Clean up storage resources.
  """
  @callback terminate(state()) :: :ok

  @optional_callbacks terminate: 1
end
```

**Step 4: Implement ETS adapter**

Create `lib/datalox/storage/ets.ex`:

```elixir
defmodule Datalox.Storage.ETS do
  @moduledoc """
  ETS-based storage backend for Datalox.

  Uses one ETS table per predicate for efficient lookups.
  Tables are created as bags to allow duplicate-free storage of facts.
  """

  @behaviour Datalox.Storage

  @type state :: %{
          tables: %{atom() => :ets.tab()},
          name: atom()
        }

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    {:ok, %{tables: %{}, name: name}}
  end

  @impl true
  def insert(state, predicate, tuple) do
    state = ensure_table(state, predicate)
    table = state.tables[predicate]
    :ets.insert(table, {tuple})
    {:ok, state}
  end

  @impl true
  def delete(state, predicate, tuple) do
    case Map.get(state.tables, predicate) do
      nil ->
        {:ok, state}

      table ->
        :ets.delete_object(table, {tuple})
        {:ok, state}
    end
  end

  @impl true
  def lookup(state, predicate, pattern) do
    case Map.get(state.tables, predicate) do
      nil ->
        {:ok, []}

      table ->
        results =
          table
          |> :ets.tab2list()
          |> Enum.filter(fn {tuple} -> matches_pattern?(tuple, pattern) end)
          |> Enum.map(fn {tuple} -> tuple end)

        {:ok, results}
    end
  end

  @impl true
  def all(state, predicate) do
    case Map.get(state.tables, predicate) do
      nil ->
        {:ok, []}

      table ->
        results = :ets.tab2list(table) |> Enum.map(fn {tuple} -> tuple end)
        {:ok, results}
    end
  end

  @impl true
  def terminate(state) do
    Enum.each(state.tables, fn {_pred, table} ->
      :ets.delete(table)
    end)

    :ok
  end

  # Ensure a table exists for the given predicate
  defp ensure_table(state, predicate) do
    if Map.has_key?(state.tables, predicate) do
      state
    else
      table_name = :"#{state.name}_#{predicate}"
      table = :ets.new(table_name, [:set, :protected])
      put_in(state.tables[predicate], table)
    end
  end

  # Check if a tuple matches a pattern (with :_ wildcards)
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

**Step 5: Run test to verify it passes**

Run: `nix develop --command mix test test/datalox/storage/ets_test.exs`

Expected: PASS

**Step 6: Commit**

```bash
git add lib/datalox/storage.ex lib/datalox/storage/ets.ex test/datalox/storage/ets_test.exs
git commit -m "feat: add Storage behaviour and ETS adapter"
```

---

### Task 5: Database GenServer

**Files:**
- Create: `lib/datalox/database.ex`
- Create: `test/datalox/database_test.exs`

**Step 1: Write failing test**

Create `test/datalox/database_test.exs`:

```elixir
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
end
```

**Step 2: Run test to verify it fails**

Run: `nix develop --command mix test test/datalox/database_test.exs`

Expected: FAIL - Database module not defined

**Step 3: Implement Database GenServer**

Create `lib/datalox/database.ex`:

```elixir
defmodule Datalox.Database do
  @moduledoc """
  A Datalox database instance.

  Each database is a GenServer that manages:
  - Base facts (asserted by users)
  - Derived facts (computed from rules)
  - Rules and their dependencies
  - Storage backend
  """

  use GenServer

  alias Datalox.Storage

  @type t :: pid()

  defstruct [
    :name,
    :storage_mod,
    :storage_state,
    rules: [],
    derived_facts: %{}
  ]

  # Client API

  @doc """
  Starts a new database.

  ## Options

    * `:name` - Required. Name for the database.
    * `:storage` - Storage module. Defaults to `Datalox.Storage.ETS`.

  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: via_tuple(name))
  end

  @doc """
  Asserts a fact into the database.
  """
  @spec assert(t() | atom(), {atom(), list()}) :: :ok | {:error, term()}
  def assert(db, {predicate, tuple}) when is_atom(predicate) and is_list(tuple) do
    GenServer.call(resolve(db), {:assert, predicate, tuple})
  end

  @doc """
  Retracts a fact from the database.
  """
  @spec retract(t() | atom(), {atom(), list()}) :: :ok | {:error, term()}
  def retract(db, {predicate, tuple}) when is_atom(predicate) and is_list(tuple) do
    GenServer.call(resolve(db), {:retract, predicate, tuple})
  end

  @doc """
  Queries facts matching a pattern.
  """
  @spec query(t() | atom(), {atom(), list()}) :: [{atom(), list()}]
  def query(db, {predicate, pattern}) when is_atom(predicate) and is_list(pattern) do
    GenServer.call(resolve(db), {:query, predicate, pattern})
  end

  @doc """
  Checks if any fact matches the pattern.
  """
  @spec exists?(t() | atom(), {atom(), list()}) :: boolean()
  def exists?(db, {predicate, pattern}) when is_atom(predicate) and is_list(pattern) do
    GenServer.call(resolve(db), {:exists, predicate, pattern})
  end

  @doc """
  Stops the database.
  """
  @spec stop(t() | atom()) :: :ok
  def stop(db) do
    GenServer.stop(resolve(db))
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    storage_mod = Keyword.get(opts, :storage, Storage.ETS)

    case storage_mod.init(name: name) do
      {:ok, storage_state} ->
        state = %__MODULE__{
          name: name,
          storage_mod: storage_mod,
          storage_state: storage_state
        }

        {:ok, state}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:assert, predicate, tuple}, _from, state) do
    case state.storage_mod.insert(state.storage_state, predicate, tuple) do
      {:ok, new_storage_state} ->
        {:reply, :ok, %{state | storage_state: new_storage_state}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:retract, predicate, tuple}, _from, state) do
    case state.storage_mod.delete(state.storage_state, predicate, tuple) do
      {:ok, new_storage_state} ->
        {:reply, :ok, %{state | storage_state: new_storage_state}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:query, predicate, pattern}, _from, state) do
    case state.storage_mod.lookup(state.storage_state, predicate, pattern) do
      {:ok, results} ->
        tagged_results = Enum.map(results, fn tuple -> {predicate, tuple} end)
        {:reply, tagged_results, state}

      {:error, _reason} ->
        {:reply, [], state}
    end
  end

  @impl true
  def handle_call({:exists, predicate, pattern}, _from, state) do
    case state.storage_mod.lookup(state.storage_state, predicate, pattern) do
      {:ok, [_ | _]} -> {:reply, true, state}
      {:ok, []} -> {:reply, false, state}
      {:error, _} -> {:reply, false, state}
    end
  end

  @impl true
  def terminate(_reason, state) do
    if function_exported?(state.storage_mod, :terminate, 1) do
      state.storage_mod.terminate(state.storage_state)
    end

    :ok
  end

  # Private helpers

  defp via_tuple(name) do
    {:via, Registry, {Datalox.Registry, name}}
  end

  defp resolve(db) when is_pid(db), do: db
  defp resolve(name) when is_atom(name), do: via_tuple(name)
end
```

**Step 4: Run test to verify it passes**

Run: `nix develop --command mix test test/datalox/database_test.exs`

Expected: PASS

**Step 5: Commit**

```bash
git add lib/datalox/database.ex test/datalox/database_test.exs
git commit -m "feat: add Database GenServer with basic fact operations"
```

---

### Task 6: Update Main Datalox Module

**Files:**
- Modify: `lib/datalox.ex`
- Modify: `test/datalox_test.exs`

**Step 1: Write failing test**

Replace `test/datalox_test.exs`:

```elixir
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
      on_exit(fn -> Datalox.stop(db) end)
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
      on_exit(fn -> Datalox.stop(db) end)
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
      on_exit(fn -> Datalox.stop(db) end)
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
      on_exit(fn -> Datalox.stop(db) end)
      {:ok, db: db}
    end

    test "checks fact existence", %{db: db} do
      refute Datalox.exists?(db, {:user, [:_, :_]})
      :ok = Datalox.assert(db, {:user, ["alice", :admin]})
      assert Datalox.exists?(db, {:user, [:_, :_]})
    end
  end
end
```

**Step 2: Run test to verify it fails**

Run: `nix develop --command mix test test/datalox_test.exs`

Expected: FAIL - functions not implemented

**Step 3: Update Datalox module**

Replace `lib/datalox.ex`:

```elixir
defmodule Datalox do
  @moduledoc """
  Datalox - A Datalog implementation in Elixir.

  Datalox provides a rule engine based on Datalog with support for:
  - Stratified negation
  - Aggregation (count, sum, min, max, avg)
  - Incremental maintenance
  - Magic Sets query optimization
  - Pluggable storage backends

  ## Quick Start

      # Create a database
      {:ok, db} = Datalox.new(name: :my_db)

      # Assert facts
      Datalox.assert(db, {:user, ["alice", :admin]})
      Datalox.assert(db, {:user, ["bob", :viewer]})

      # Query
      Datalox.query(db, {:user, [:_, :admin]})
      #=> [{:user, ["alice", :admin]}]

  """

  alias Datalox.Database

  @doc """
  Creates a new Datalox database.

  ## Options

    * `:name` - Required. The name to register the database under.
    * `:storage` - Storage backend module. Defaults to `Datalox.Storage.ETS`.

  ## Examples

      {:ok, db} = Datalox.new(name: :my_db)

  """
  @spec new(keyword()) :: {:ok, pid()} | {:error, term()}
  def new(opts) do
    Database.start_link(opts)
  end

  @doc """
  Stops a database.
  """
  @spec stop(pid() | atom()) :: :ok
  def stop(db) do
    Database.stop(db)
  end

  @doc """
  Asserts a fact into the database.

  ## Examples

      Datalox.assert(db, {:user, ["alice", :admin]})

  """
  @spec assert(pid() | atom(), {atom(), list()}) :: :ok | {:error, term()}
  def assert(db, fact) do
    Database.assert(db, fact)
  end

  @doc """
  Asserts multiple facts into the database.

  ## Examples

      Datalox.assert_all(db, [
        {:user, ["alice", :admin]},
        {:user, ["bob", :viewer]}
      ])

  """
  @spec assert_all(pid() | atom(), [{atom(), list()}]) :: :ok | {:error, term()}
  def assert_all(db, facts) when is_list(facts) do
    Enum.each(facts, &assert(db, &1))
    :ok
  end

  @doc """
  Retracts a fact from the database.

  ## Examples

      Datalox.retract(db, {:user, ["alice", :admin]})

  """
  @spec retract(pid() | atom(), {atom(), list()}) :: :ok | {:error, term()}
  def retract(db, fact) do
    Database.retract(db, fact)
  end

  @doc """
  Queries facts matching a pattern. Use `:_` for wildcards.

  ## Examples

      Datalox.query(db, {:user, [:_, :admin]})
      #=> [{:user, ["alice", :admin]}]

  """
  @spec query(pid() | atom(), {atom(), list()}) :: [{atom(), list()}]
  def query(db, pattern) do
    Database.query(db, pattern)
  end

  @doc """
  Returns the first fact matching the pattern, or nil.

  ## Examples

      Datalox.query_one(db, {:user, ["alice", :_]})
      #=> {:user, ["alice", :admin]}

  """
  @spec query_one(pid() | atom(), {atom(), list()}) :: {atom(), list()} | nil
  def query_one(db, pattern) do
    case query(db, pattern) do
      [first | _] -> first
      [] -> nil
    end
  end

  @doc """
  Checks if any fact matches the pattern.

  ## Examples

      Datalox.exists?(db, {:user, ["alice", :_]})
      #=> true

  """
  @spec exists?(pid() | atom(), {atom(), list()}) :: boolean()
  def exists?(db, pattern) do
    Database.exists?(db, pattern)
  end
end
```

**Step 4: Run test to verify it passes**

Run: `nix develop --command mix test`

Expected: All tests pass

**Step 5: Commit**

```bash
git add lib/datalox.ex test/datalox_test.exs
git commit -m "feat: complete main Datalox module with public API"
```

---

## Phase 2: Rule Evaluation

### Task 7: Stratification Analysis

**Files:**
- Create: `lib/datalox/optimizer/stratifier.ex`
- Create: `test/datalox/optimizer/stratifier_test.exs`

**Step 1: Write failing test**

Create `test/datalox/optimizer/stratifier_test.exs`:

```elixir
defmodule Datalox.Optimizer.StratifierTest do
  use ExUnit.Case, async: true

  alias Datalox.Optimizer.Stratifier
  alias Datalox.Rule

  describe "stratify/1" do
    test "stratifies rules without negation" do
      rules = [
        Rule.new({:ancestor, [:X, :Z]}, [{:parent, [:X, :Z]}]),
        Rule.new({:ancestor, [:X, :Z]}, [{:parent, [:X, :Y]}, {:ancestor, [:Y, :Z]}])
      ]

      assert {:ok, strata} = Stratifier.stratify(rules)
      assert length(strata) == 1
      assert length(hd(strata)) == 2
    end

    test "stratifies rules with negation" do
      rules = [
        Rule.new({:active, [:User]}, [{:user, [:User]}], negations: [{:banned, [:User]}]),
        Rule.new({:banned, [:User]}, [{:violation, [:User, :_]}])
      ]

      assert {:ok, strata} = Stratifier.stratify(rules)
      # banned must be in earlier stratum than active
      assert length(strata) == 2
    end

    test "detects circular negation" do
      rules = [
        Rule.new({:a, [:X]}, [{:b, [:X]}], negations: [{:c, [:X]}]),
        Rule.new({:c, [:X]}, [{:d, [:X]}], negations: [{:a, [:X]}])
      ]

      assert {:error, {:circular_negation, cycle}} = Stratifier.stratify(rules)
      assert :a in cycle
      assert :c in cycle
    end

    test "handles base predicates" do
      rules = [
        Rule.new({:can_access, [:U, :R]}, [
          {:user, [:U, :Role]},
          {:permission, [:Role, :R]}
        ])
      ]

      assert {:ok, strata} = Stratifier.stratify(rules)
      assert length(strata) == 1
    end
  end

  describe "dependency_graph/1" do
    test "builds predicate dependency graph" do
      rules = [
        Rule.new({:ancestor, [:X, :Z]}, [{:parent, [:X, :Y]}, {:ancestor, [:Y, :Z]}]),
        Rule.new({:can_reach, [:X, :Y]}, [{:ancestor, [:X, :Y]}])
      ]

      graph = Stratifier.dependency_graph(rules)

      assert :parent in graph[:ancestor].positive
      assert :ancestor in graph[:ancestor].positive
      assert :ancestor in graph[:can_reach].positive
    end

    test "tracks negative dependencies" do
      rules = [
        Rule.new({:active, [:U]}, [{:user, [:U]}], negations: [{:banned, [:U]}])
      ]

      graph = Stratifier.dependency_graph(rules)

      assert :user in graph[:active].positive
      assert :banned in graph[:active].negative
    end
  end
end
```

**Step 2: Run test to verify it fails**

Run: `nix develop --command mix test test/datalox/optimizer/stratifier_test.exs`

Expected: FAIL - module not defined

**Step 3: Implement Stratifier**

Create `lib/datalox/optimizer/stratifier.ex`:

```elixir
defmodule Datalox.Optimizer.Stratifier do
  @moduledoc """
  Computes stratification for Datalog rules with negation.

  Stratification partitions rules into layers (strata) such that:
  1. If rule R uses predicate P positively, P can be in same or earlier stratum
  2. If rule R uses predicate P negatively, P must be in strictly earlier stratum

  This ensures negation is "safe" - we only negate predicates whose
  complete extension is already computed.
  """

  alias Datalox.Rule

  @type predicate :: atom()
  @type stratum :: [Rule.t()]
  @type dependency :: %{positive: MapSet.t(predicate()), negative: MapSet.t(predicate())}

  @doc """
  Stratifies a list of rules.

  Returns `{:ok, strata}` where strata is a list of rule lists,
  ordered from first to evaluate to last.

  Returns `{:error, {:circular_negation, cycle}}` if rules cannot be stratified.
  """
  @spec stratify([Rule.t()]) :: {:ok, [stratum()]} | {:error, {:circular_negation, [predicate()]}}
  def stratify(rules) do
    graph = dependency_graph(rules)
    predicates = Map.keys(graph)

    case compute_strata(predicates, graph) do
      {:ok, predicate_strata} ->
        rule_strata = assign_rules_to_strata(rules, predicate_strata)
        {:ok, rule_strata}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Builds a dependency graph from rules.

  Returns a map from predicate -> %{positive: MapSet, negative: MapSet}
  """
  @spec dependency_graph([Rule.t()]) :: %{predicate() => dependency()}
  def dependency_graph(rules) do
    rules
    |> Enum.reduce(%{}, fn rule, acc ->
      {head_pred, _} = rule.head
      deps = Rule.depends_on(rule)

      positive_deps =
        rule.body
        |> Enum.map(fn {pred, _} -> pred end)
        |> MapSet.new()

      negative_deps =
        rule.negations
        |> Enum.map(fn {pred, _} -> pred end)
        |> MapSet.new()

      current = Map.get(acc, head_pred, %{positive: MapSet.new(), negative: MapSet.new()})

      updated = %{
        positive: MapSet.union(current.positive, positive_deps),
        negative: MapSet.union(current.negative, negative_deps)
      }

      # Also ensure all referenced predicates are in the graph
      acc = Map.put(acc, head_pred, updated)

      Enum.reduce(deps, acc, fn dep, inner_acc ->
        Map.put_new(inner_acc, dep, %{positive: MapSet.new(), negative: MapSet.new()})
      end)
    end)
  end

  # Compute stratum assignment for each predicate
  defp compute_strata(predicates, graph) do
    initial_strata = Map.new(predicates, fn p -> {p, 0} end)
    iterate_strata(initial_strata, graph, length(predicates) + 1)
  end

  defp iterate_strata(strata, _graph, 0) do
    # If we've iterated more than |predicates| times, we have a cycle
    {:error, {:circular_negation, find_cycle(strata)}}
  end

  defp iterate_strata(strata, graph, remaining) do
    new_strata =
      Enum.reduce(strata, strata, fn {pred, _stratum}, acc ->
        case Map.get(graph, pred) do
          nil ->
            acc

          %{positive: pos, negative: neg} ->
            # Stratum must be >= max of positive dependencies
            pos_max =
              pos
              |> Enum.map(&Map.get(strata, &1, 0))
              |> Enum.max(fn -> 0 end)

            # Stratum must be > max of negative dependencies
            neg_max =
              neg
              |> Enum.map(&Map.get(strata, &1, 0))
              |> Enum.max(fn -> -1 end)

            new_stratum = max(pos_max, neg_max + 1)
            Map.put(acc, pred, max(Map.get(acc, pred, 0), new_stratum))
        end
      end)

    if new_strata == strata do
      {:ok, strata}
    else
      iterate_strata(new_strata, graph, remaining - 1)
    end
  end

  # Find predicates involved in the negative cycle
  defp find_cycle(strata) do
    strata
    |> Enum.filter(fn {_pred, stratum} -> stratum > length(Map.keys(strata)) end)
    |> Enum.map(fn {pred, _} -> pred end)
  end

  # Group rules by their head predicate's stratum
  defp assign_rules_to_strata(rules, predicate_strata) do
    max_stratum =
      predicate_strata
      |> Map.values()
      |> Enum.max(fn -> 0 end)

    0..max_stratum
    |> Enum.map(fn stratum ->
      Enum.filter(rules, fn rule ->
        {head_pred, _} = rule.head
        Map.get(predicate_strata, head_pred, 0) == stratum
      end)
    end)
    |> Enum.reject(&Enum.empty?/1)
  end
end
```

**Step 4: Run test to verify it passes**

Run: `nix develop --command mix test test/datalox/optimizer/stratifier_test.exs`

Expected: PASS

**Step 5: Commit**

```bash
git add lib/datalox/optimizer/stratifier.ex test/datalox/optimizer/stratifier_test.exs
git commit -m "feat: add stratification analysis for negation safety"
```

---

### Task 8: Semi-Naive Evaluator

**Files:**
- Create: `lib/datalox/evaluator.ex`
- Create: `test/datalox/evaluator_test.exs`

**Step 1: Write failing test**

Create `test/datalox/evaluator_test.exs`:

```elixir
defmodule Datalox.EvaluatorTest do
  use ExUnit.Case, async: true

  alias Datalox.Evaluator
  alias Datalox.Rule
  alias Datalox.Storage.ETS

  setup do
    {:ok, storage} = ETS.init(name: :"test_#{:erlang.unique_integer()}")
    on_exit(fn -> ETS.terminate(storage) end)
    {:ok, storage: storage}
  end

  describe "evaluate/3 with simple rules" do
    test "derives facts from single rule", %{storage: storage} do
      # Facts: parent(alice, bob), parent(bob, carol)
      {:ok, storage} = ETS.insert(storage, :parent, ["alice", "bob"])
      {:ok, storage} = ETS.insert(storage, :parent, ["bob", "carol"])

      # Rule: ancestor(X, Y) :- parent(X, Y)
      rules = [
        Rule.new({:ancestor, [:X, :Y]}, [{:parent, [:X, :Y]}])
      ]

      {:ok, derived, _storage} = Evaluator.evaluate(rules, storage, ETS)

      assert {:ancestor, ["alice", "bob"]} in derived
      assert {:ancestor, ["bob", "carol"]} in derived
    end

    test "handles recursive rules", %{storage: storage} do
      # Facts: parent(alice, bob), parent(bob, carol)
      {:ok, storage} = ETS.insert(storage, :parent, ["alice", "bob"])
      {:ok, storage} = ETS.insert(storage, :parent, ["bob", "carol"])

      # Rules:
      # ancestor(X, Y) :- parent(X, Y)
      # ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z)
      rules = [
        Rule.new({:ancestor, [:X, :Y]}, [{:parent, [:X, :Y]}]),
        Rule.new({:ancestor, [:X, :Z]}, [{:parent, [:X, :Y]}, {:ancestor, [:Y, :Z]}])
      ]

      {:ok, derived, _storage} = Evaluator.evaluate(rules, storage, ETS)

      assert {:ancestor, ["alice", "bob"]} in derived
      assert {:ancestor, ["bob", "carol"]} in derived
      assert {:ancestor, ["alice", "carol"]} in derived
    end
  end

  describe "evaluate/3 with negation" do
    test "handles stratified negation", %{storage: storage} do
      # Facts
      {:ok, storage} = ETS.insert(storage, :user, ["alice"])
      {:ok, storage} = ETS.insert(storage, :user, ["bob"])
      {:ok, storage} = ETS.insert(storage, :banned, ["bob"])

      # Rule: active(X) :- user(X), not banned(X)
      rules = [
        Rule.new({:active, [:X]}, [{:user, [:X]}], negations: [{:banned, [:X]}])
      ]

      {:ok, derived, _storage} = Evaluator.evaluate(rules, storage, ETS)

      assert {:active, ["alice"]} in derived
      refute {:active, ["bob"]} in derived
    end
  end

  describe "evaluate/3 with joins" do
    test "handles multi-predicate joins", %{storage: storage} do
      # Facts
      {:ok, storage} = ETS.insert(storage, :user, ["alice", :admin])
      {:ok, storage} = ETS.insert(storage, :user, ["bob", :viewer])
      {:ok, storage} = ETS.insert(storage, :permission, [:admin, "doc1"])
      {:ok, storage} = ETS.insert(storage, :permission, [:admin, "doc2"])

      # Rule: can_access(U, D) :- user(U, R), permission(R, D)
      rules = [
        Rule.new({:can_access, [:U, :D]}, [{:user, [:U, :R]}, {:permission, [:R, :D]}])
      ]

      {:ok, derived, _storage} = Evaluator.evaluate(rules, storage, ETS)

      assert {:can_access, ["alice", "doc1"]} in derived
      assert {:can_access, ["alice", "doc2"]} in derived
      refute Enum.any?(derived, fn {pred, [user, _]} -> pred == :can_access and user == "bob" end)
    end
  end
end
```

**Step 2: Run test to verify it fails**

Run: `nix develop --command mix test test/datalox/evaluator_test.exs`

Expected: FAIL - Evaluator module not defined

**Step 3: Implement Evaluator**

Create `lib/datalox/evaluator.ex`:

```elixir
defmodule Datalox.Evaluator do
  @moduledoc """
  Semi-naive evaluation for Datalog rules.

  Semi-naive evaluation is an optimization over naive evaluation that
  avoids redundant computation by only considering "new" facts from
  the previous iteration (delta facts).
  """

  alias Datalox.Rule
  alias Datalox.Optimizer.Stratifier

  @type fact :: {atom(), list()}
  @type storage :: any()

  @doc """
  Evaluates rules against the current fact base and returns derived facts.

  Returns `{:ok, derived_facts, updated_storage}` on success.
  """
  @spec evaluate([Rule.t()], storage(), module()) ::
          {:ok, [fact()], storage()} | {:error, term()}
  def evaluate(rules, storage, storage_mod) do
    case Stratifier.stratify(rules) do
      {:ok, strata} ->
        evaluate_strata(strata, storage, storage_mod, [])

      {:error, _} = error ->
        error
    end
  end

  # Evaluate each stratum in order
  defp evaluate_strata([], storage, _storage_mod, acc) do
    {:ok, acc, storage}
  end

  defp evaluate_strata([stratum | rest], storage, storage_mod, acc) do
    {derived, storage} = evaluate_stratum(stratum, storage, storage_mod)
    evaluate_strata(rest, storage, storage_mod, acc ++ derived)
  end

  # Evaluate a single stratum using semi-naive iteration
  defp evaluate_stratum(rules, storage, storage_mod) do
    # Initial pass - derive all facts we can
    {initial_delta, storage} = derive_all(rules, storage, storage_mod)

    # Iterate until fixpoint
    iterate_seminaive(rules, storage, storage_mod, initial_delta, initial_delta)
  end

  defp iterate_seminaive(_rules, storage, _storage_mod, _delta, all_derived)
       when map_size(all_derived) == 0 or all_derived == [] do
    {Map.values(all_derived) |> List.flatten(), storage}
  end

  defp iterate_seminaive(rules, storage, storage_mod, delta, all_derived) when is_list(delta) do
    delta_map = Enum.group_by(delta, fn {pred, _} -> pred end)
    iterate_seminaive(rules, storage, storage_mod, delta_map, list_to_derived_map(delta))
  end

  defp iterate_seminaive(rules, storage, storage_mod, delta, all_derived) do
    # Derive new facts using delta
    {new_facts, storage} = derive_with_delta(rules, storage, storage_mod, delta)

    # Filter out facts we've already derived
    new_delta =
      new_facts
      |> Enum.reject(fn fact -> Map.has_key?(all_derived, fact) end)

    if Enum.empty?(new_delta) do
      {Map.keys(all_derived), storage}
    else
      new_derived = Enum.reduce(new_delta, all_derived, fn fact, acc -> Map.put(acc, fact, true) end)
      new_delta_map = Enum.group_by(new_delta, fn {pred, _} -> pred end)
      iterate_seminaive(rules, storage, storage_mod, new_delta_map, new_derived)
    end
  end

  defp list_to_derived_map(facts) do
    Enum.reduce(facts, %{}, fn fact, acc -> Map.put(acc, fact, true) end)
  end

  # Initial derivation pass
  defp derive_all(rules, storage, storage_mod) do
    Enum.reduce(rules, {[], storage}, fn rule, {facts, st} ->
      {new_facts, st} = evaluate_rule(rule, st, storage_mod, %{})
      {facts ++ new_facts, st}
    end)
  end

  # Derive using delta facts
  defp derive_with_delta(rules, storage, storage_mod, delta) do
    Enum.reduce(rules, {[], storage}, fn rule, {facts, st} ->
      {new_facts, st} = evaluate_rule(rule, st, storage_mod, delta)
      {facts ++ new_facts, st}
    end)
  end

  # Evaluate a single rule
  defp evaluate_rule(rule, storage, storage_mod, _delta) do
    # Get all bindings from the body
    bindings = evaluate_body(rule.body, storage, storage_mod, [%{}])

    # Filter by negations
    bindings = filter_negations(bindings, rule.negations, storage, storage_mod)

    # Project to head variables and create facts
    facts =
      bindings
      |> Enum.map(fn binding -> instantiate_head(rule.head, binding) end)
      |> Enum.uniq()

    # Store derived facts
    storage =
      Enum.reduce(facts, storage, fn {pred, tuple}, st ->
        {:ok, st} = storage_mod.insert(st, pred, tuple)
        st
      end)

    {facts, storage}
  end

  # Evaluate body goals, building up bindings
  defp evaluate_body([], _storage, _storage_mod, bindings), do: bindings

  defp evaluate_body([goal | rest], storage, storage_mod, bindings) do
    new_bindings =
      Enum.flat_map(bindings, fn binding ->
        evaluate_goal(goal, storage, storage_mod, binding)
      end)

    evaluate_body(rest, storage, storage_mod, new_bindings)
  end

  # Evaluate a single goal against current bindings
  defp evaluate_goal({predicate, terms}, storage, storage_mod, binding) do
    # Substitute known bindings into pattern
    pattern = Enum.map(terms, fn term -> substitute(term, binding) end)

    # Query storage
    {:ok, results} = storage_mod.lookup(storage, predicate, pattern)

    # Unify results with pattern to extend bindings
    Enum.flat_map(results, fn tuple ->
      case unify(terms, tuple, binding) do
        {:ok, new_binding} -> [new_binding]
        :fail -> []
      end
    end)
  end

  # Filter bindings by checking negations don't match
  defp filter_negations(bindings, [], _storage, _storage_mod), do: bindings

  defp filter_negations(bindings, negations, storage, storage_mod) do
    Enum.filter(bindings, fn binding ->
      Enum.all?(negations, fn {pred, terms} ->
        pattern = Enum.map(terms, fn term -> substitute(term, binding) end)
        {:ok, results} = storage_mod.lookup(storage, pred, pattern)
        Enum.empty?(results)
      end)
    end)
  end

  # Substitute variables in term using binding
  defp substitute(term, binding) when is_atom(term) do
    if is_variable?(term) do
      Map.get(binding, term, :_)
    else
      term
    end
  end

  defp substitute(term, _binding), do: term

  # Unify terms with values, extending binding
  defp unify([], [], binding), do: {:ok, binding}

  defp unify([term | terms], [value | values], binding) do
    case unify_one(term, value, binding) do
      {:ok, new_binding} -> unify(terms, values, new_binding)
      :fail -> :fail
    end
  end

  defp unify_one(:_, _value, binding), do: {:ok, binding}

  defp unify_one(term, value, binding) when is_atom(term) do
    if is_variable?(term) do
      case Map.fetch(binding, term) do
        {:ok, ^value} -> {:ok, binding}
        {:ok, _other} -> :fail
        :error -> {:ok, Map.put(binding, term, value)}
      end
    else
      if term == value, do: {:ok, binding}, else: :fail
    end
  end

  defp unify_one(term, value, binding) do
    if term == value, do: {:ok, binding}, else: :fail
  end

  # Instantiate head with binding
  defp instantiate_head({predicate, terms}, binding) do
    values = Enum.map(terms, fn term -> substitute_value(term, binding) end)
    {predicate, values}
  end

  defp substitute_value(term, binding) when is_atom(term) do
    if is_variable?(term) do
      Map.fetch!(binding, term)
    else
      term
    end
  end

  defp substitute_value(term, _binding), do: term

  # Check if term is a variable (uppercase atom)
  defp is_variable?(term) when is_atom(term) do
    str = Atom.to_string(term)
    String.match?(str, ~r/^[A-Z]/)
  end

  defp is_variable?(_), do: false
end
```

**Step 4: Run test to verify it passes**

Run: `nix develop --command mix test test/datalox/evaluator_test.exs`

Expected: PASS

**Step 5: Commit**

```bash
git add lib/datalox/evaluator.ex test/datalox/evaluator_test.exs
git commit -m "feat: add semi-naive Datalog evaluator"
```

---

### Task 9: Integrate Evaluator with Database

**Files:**
- Modify: `lib/datalox/database.ex`
- Modify: `test/datalox/database_test.exs`

**Step 1: Write failing test**

Add to `test/datalox/database_test.exs`:

```elixir
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
```

**Step 2: Run test to verify it fails**

Run: `nix develop --command mix test test/datalox/database_test.exs`

Expected: FAIL - load_rules not defined

**Step 3: Update Database module**

Add to `lib/datalox/database.ex` in the Client API section:

```elixir
  @doc """
  Loads rules into the database.
  """
  @spec load_rules(t() | atom(), [Rule.t()]) :: :ok | {:error, term()}
  def load_rules(db, rules) when is_list(rules) do
    GenServer.call(resolve(db), {:load_rules, rules})
  end
```

Add alias at top:

```elixir
  alias Datalox.{Storage, Rule, Evaluator}
```

Add handler in Server Callbacks:

```elixir
  @impl true
  def handle_call({:load_rules, rules}, _from, state) do
    case Evaluator.evaluate(rules, state.storage_state, state.storage_mod) do
      {:ok, _derived, new_storage_state} ->
        new_state = %{state | rules: rules, storage_state: new_storage_state}
        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end
```

**Step 4: Run test to verify it passes**

Run: `nix develop --command mix test test/datalox/database_test.exs`

Expected: PASS

**Step 5: Run all tests**

Run: `nix develop --command mix test`

Expected: All tests pass

**Step 6: Commit**

```bash
git add lib/datalox/database.ex test/datalox/database_test.exs
git commit -m "feat: integrate rule evaluator with database"
```

---

## Phase 3: DSL and Parser

### Task 10: Macro DSL for Rules

**Files:**
- Create: `lib/datalox/dsl.ex`
- Create: `test/datalox/dsl_test.exs`

**Step 1: Write failing test**

Create `test/datalox/dsl_test.exs`:

```elixir
defmodule Datalox.DSLTest do
  use ExUnit.Case, async: true

  describe "deffact/1" do
    test "defines facts" do
      defmodule TestFacts do
        use Datalox.DSL

        deffact user("alice", :admin)
        deffact user("bob", :viewer)
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

      assert rule.head == {:ancestor, [:x, :y]}
      assert rule.body == [{:parent, [:x, :y]}]
    end

    test "defines rules with multiple body goals" do
      defmodule TestMultiBody do
        use Datalox.DSL

        defrule can_access(user, resource) do
          user(user, role) and permission(role, resource)
        end
      end

      [rule] = TestMultiBody.__datalox_rules__()

      assert rule.head == {:can_access, [:user, :resource]}
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

      assert rule.body == [{:user, [:user]}]
      assert rule.negations == [{:banned, [:user]}]
    end
  end

  describe "integration" do
    test "loads DSL module into database" do
      defmodule IntegrationRules do
        use Datalox.DSL

        deffact parent("alice", "bob")
        deffact parent("bob", "carol")

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
```

**Step 2: Run test to verify it fails**

Run: `nix develop --command mix test test/datalox/dsl_test.exs`

Expected: FAIL - DSL module not defined

**Step 3: Implement DSL module**

Create `lib/datalox/dsl.ex`:

```elixir
defmodule Datalox.DSL do
  @moduledoc """
  Macro-based DSL for defining Datalog facts and rules.

  ## Usage

      defmodule MyRules do
        use Datalox.DSL

        deffact user("alice", :admin)
        deffact user("bob", :viewer)

        defrule can_access(user, resource) do
          user(user, role) and permission(role, resource)
        end

        defrule active(user) do
          user(user) and not banned(user)
        end
      end

  """

  defmacro __using__(_opts) do
    quote do
      import Datalox.DSL
      Module.register_attribute(__MODULE__, :datalox_facts, accumulate: true)
      Module.register_attribute(__MODULE__, :datalox_rules, accumulate: true)

      @before_compile Datalox.DSL
    end
  end

  defmacro __before_compile__(_env) do
    quote do
      def __datalox_facts__ do
        @datalox_facts |> Enum.reverse()
      end

      def __datalox_rules__ do
        @datalox_rules |> Enum.reverse()
      end
    end
  end

  @doc """
  Defines a fact.

  ## Example

      deffact user("alice", :admin)

  """
  defmacro deffact(fact) do
    {predicate, args} = parse_goal(fact)
    values = Enum.map(args, &Macro.escape/1)

    quote do
      @datalox_facts {unquote(predicate), unquote(values)}
    end
  end

  @doc """
  Defines a rule.

  ## Example

      defrule ancestor(x, y) do
        parent(x, y)
      end

  """
  defmacro defrule(head, do: body) do
    {head_pred, head_args} = parse_goal(head)
    head_vars = Enum.map(head_args, &var_to_atom/1)

    {body_goals, negations} = parse_body(body)

    quote do
      @datalox_rules %Datalox.Rule{
        head: {unquote(head_pred), unquote(head_vars)},
        body: unquote(Macro.escape(body_goals)),
        negations: unquote(Macro.escape(negations)),
        aggregations: [],
        guards: []
      }
    end
  end

  # Parse a goal like `user("alice", :admin)` into {:user, ["alice", :admin]}
  defp parse_goal({predicate, _, args}) when is_atom(predicate) do
    {predicate, args || []}
  end

  # Convert variable AST to atom
  defp var_to_atom({name, _, _}) when is_atom(name), do: name
  defp var_to_atom(literal), do: literal

  # Parse body into goals and negations
  defp parse_body({:and, _, [left, right]}) do
    {left_goals, left_negs} = parse_body(left)
    {right_goals, right_negs} = parse_body(right)
    {left_goals ++ right_goals, left_negs ++ right_negs}
  end

  defp parse_body({:not, _, [goal]}) do
    {predicate, args} = parse_goal(goal)
    vars = Enum.map(args, &var_to_atom/1)
    {[], [{predicate, vars}]}
  end

  defp parse_body(goal) do
    {predicate, args} = parse_goal(goal)
    vars = Enum.map(args, &var_to_atom/1)
    {[{predicate, vars}], []}
  end
end
```

**Step 4: Add load_module to Datalox**

Add to `lib/datalox.ex`:

```elixir
  @doc """
  Loads facts and rules from a DSL module.

  ## Examples

      Datalox.load_module(db, MyRules)

  """
  @spec load_module(pid() | atom(), module()) :: :ok | {:error, term()}
  def load_module(db, module) when is_atom(module) do
    facts = module.__datalox_facts__()
    rules = module.__datalox_rules__()

    with :ok <- assert_all(db, facts),
         :ok <- Database.load_rules(db, rules) do
      :ok
    end
  end
```

**Step 5: Run test to verify it passes**

Run: `nix develop --command mix test test/datalox/dsl_test.exs`

Expected: PASS

**Step 6: Commit**

```bash
git add lib/datalox/dsl.ex lib/datalox.ex test/datalox/dsl_test.exs
git commit -m "feat: add macro DSL for defining facts and rules"
```

---

## Summary

This plan covers the core foundation (Tasks 1-10):
- Project setup with dependencies
- Error types
- Rule data structure
- Storage behaviour and ETS adapter
- Database GenServer
- Stratification analysis
- Semi-naive evaluator
- DSL for facts and rules

**Remaining phases (to be planned in follow-up):**
- Phase 4: Parser for .dl files (NimbleParsec)
- Phase 5: Aggregation support
- Phase 6: Incremental maintenance
- Phase 7: Magic Sets optimization
- Phase 8: Observability (explain, metrics, subscriptions)
- Phase 9: Documentation and property tests

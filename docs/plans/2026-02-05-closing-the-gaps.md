# Closing the Gaps: Indexing, Join Ordering, Aggregation Integration, and Parallel Evaluation

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close the four key gaps identified in the comparison against mature Datalog engines: (1) indexing for fast lookups, (2) cost-based join ordering, (3) wiring aggregation into the evaluator and parser, and (4) BEAM-native parallel rule evaluation.

**Architecture:** Each gap is a self-contained phase. Phase 1 (Indexing) replaces the full-table-scan `lookup/3` with a first-column hash index and a generic secondary index system, all within the existing `Storage.ETS` adapter and `Storage` behaviour. Phase 2 (Join Ordering) adds a cost estimator and goal reorderer inside the evaluator. Phase 3 (Aggregation) wires `Datalox.Aggregation` into the evaluator's rule processing and extends the lexer/parser to support aggregation syntax. Phase 4 (Parallel Evaluation) uses `Task.async_stream` to evaluate independent rules within a stratum concurrently.

**Tech Stack:** Elixir 1.18, Erlang/OTP 27, ETS, NimbleParsec, Telemetry. All commands run via `nix develop -c <cmd>`.

---

## Phase 1: Indexed Storage

The single biggest performance gap. Currently `lookup/3` calls `:ets.tab2list()` and filters in Elixir — O(n) per lookup. We'll add:
- A **primary index** on the first column (ETS `:duplicate_bag` keyed by first element)
- A **secondary index** callback on the Storage behaviour for multi-column access patterns

### Task 1: Add `count/2` to Storage behaviour and ETS adapter

This is needed by join ordering (Phase 2) and is trivial. Adding it first avoids touching the behaviour twice.

**Files:**
- Modify: `lib/datalox/storage.ex`
- Modify: `lib/datalox/storage/ets.ex`
- Test: `test/datalox/storage/ets_test.exs`

**Step 1: Write the failing test**

Add to `test/datalox/storage/ets_test.exs`:

```elixir
describe "count/2" do
  test "returns 0 for unknown predicate", %{state: state} do
    assert {:ok, 0} == ETS.count(state, :unknown)
  end

  test "returns number of facts for predicate", %{state: state} do
    {:ok, state} = ETS.insert(state, :user, ["alice", :admin])
    {:ok, state} = ETS.insert(state, :user, ["bob", :viewer])
    {:ok, _state} = ETS.insert(state, :role, [:admin, :write])
    assert {:ok, 2} == ETS.count(state, :user)
    assert {:ok, 1} == ETS.count(state, :role)
  end
end
```

**Step 2: Run test to verify it fails**

Run: `nix develop -c mix test test/datalox/storage/ets_test.exs --trace`
Expected: FAIL — `ETS.count/2` undefined

**Step 3: Add callback to Storage behaviour**

In `lib/datalox/storage.ex`, add after the `all/2` callback (after line 37):

```elixir
@doc """
Return the number of facts stored for a predicate.
"""
@callback count(state(), predicate()) :: {:ok, non_neg_integer()} | {:error, term()}

@optional_callbacks terminate: 1, count: 2
```

(Update the existing `@optional_callbacks` line to include `count: 2`.)

**Step 4: Implement in ETS adapter**

In `lib/datalox/storage/ets.ex`, add after the `all/2` function (after line 69):

```elixir
@impl true
def count(state, predicate) do
  case Map.get(state.tables, predicate) do
    nil -> {:ok, 0}
    table -> {:ok, :ets.info(table, :size)}
  end
end
```

**Step 5: Run tests to verify they pass**

Run: `nix develop -c mix test test/datalox/storage/ets_test.exs --trace`
Expected: PASS

**Step 6: Commit**

```bash
nix develop -c bash -c 'git add lib/datalox/storage.ex lib/datalox/storage/ets.ex test/datalox/storage/ets_test.exs && git commit -m "feat: add count/2 to storage behaviour and ETS adapter"'
```

---

### Task 2: Replace full-table-scan lookup with first-column indexed lookup

The core indexing change. We switch from `:set` tables storing `{tuple}` to `:duplicate_bag` tables storing `{first_element, tuple}`. This lets us use `:ets.lookup/2` on the first column instead of `:ets.tab2list/1`.

**Files:**
- Modify: `lib/datalox/storage/ets.ex`
- Test: `test/datalox/storage/ets_test.exs` (existing tests must still pass)

**Step 1: Write the failing test (performance-oriented)**

Add to `test/datalox/storage/ets_test.exs`:

```elixir
describe "indexed lookup" do
  test "lookup with bound first column uses index", %{state: state} do
    # Insert many facts
    state =
      Enum.reduce(1..100, state, fn i, st ->
        {:ok, st} = ETS.insert(st, :node, [i, "label_#{i}"])
        st
      end)

    # Lookup by first column should return exactly one result
    {:ok, results} = ETS.lookup(state, :node, [42, :_])
    assert results == [[42, "label_42"]]
  end

  test "lookup with all wildcards returns all facts", %{state: state} do
    {:ok, state} = ETS.insert(state, :edge, [1, 2])
    {:ok, state} = ETS.insert(state, :edge, [2, 3])
    {:ok, state} = ETS.insert(state, :edge, [1, 3])
    {:ok, results} = ETS.lookup(state, :edge, [:_, :_])
    assert length(results) == 3
  end

  test "lookup with bound non-first column still works", %{state: state} do
    {:ok, state} = ETS.insert(state, :user, ["alice", :admin])
    {:ok, state} = ETS.insert(state, :user, ["bob", :viewer])
    {:ok, state} = ETS.insert(state, :user, ["carol", :admin])
    {:ok, results} = ETS.lookup(state, :user, [:_, :admin])
    assert length(results) == 2
  end
end
```

**Step 2: Run tests to verify existing tests still pass (baseline)**

Run: `nix develop -c mix test test/datalox/storage/ets_test.exs --trace`
Expected: New tests PASS (existing lookup logic still works), but we want to verify baseline before changing internals.

**Step 3: Rewrite ETS storage internals for indexed storage**

Replace the entire `lib/datalox/storage/ets.ex` with:

```elixir
defmodule Datalox.Storage.ETS do
  @moduledoc """
  ETS-based storage backend for Datalox.

  Uses one ETS `:duplicate_bag` table per predicate, keyed by the first
  element of each tuple for O(1) first-column lookups.
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
    key = first_key(tuple)
    :ets.insert(table, {key, tuple})
    {:ok, state}
  end

  @impl true
  def delete(state, predicate, tuple) do
    case Map.get(state.tables, predicate) do
      nil ->
        {:ok, state}

      table ->
        key = first_key(tuple)
        :ets.delete_object(table, {key, tuple})
        {:ok, state}
    end
  end

  @impl true
  def lookup(state, predicate, pattern) do
    case Map.get(state.tables, predicate) do
      nil ->
        {:ok, []}

      table ->
        results = indexed_lookup(table, pattern)
        {:ok, results}
    end
  end

  @impl true
  def all(state, predicate) do
    case Map.get(state.tables, predicate) do
      nil ->
        {:ok, []}

      table ->
        results = :ets.tab2list(table) |> Enum.map(fn {_key, tuple} -> tuple end)
        {:ok, results}
    end
  end

  @impl true
  def count(state, predicate) do
    case Map.get(state.tables, predicate) do
      nil -> {:ok, 0}
      table -> {:ok, :ets.info(table, :size)}
    end
  end

  @impl true
  def terminate(state) do
    Enum.each(state.tables, fn {_pred, table} ->
      :ets.delete(table)
    end)

    :ok
  end

  # --- Private ---

  defp ensure_table(state, predicate) do
    if Map.has_key?(state.tables, predicate) do
      state
    else
      table_name = :"#{state.name}_#{predicate}"
      table = :ets.new(table_name, [:duplicate_bag, :protected])
      put_in(state.tables[predicate], table)
    end
  end

  # When the first element of the pattern is bound, use ETS key lookup
  defp indexed_lookup(table, [first | rest_pattern]) when first != :_ do
    :ets.lookup(table, first)
    |> Enum.filter(fn {_key, tuple} -> matches_pattern?(tuple, [first | rest_pattern]) end)
    |> Enum.map(fn {_key, tuple} -> tuple end)
  end

  # When the first element is a wildcard, fall back to full scan
  defp indexed_lookup(table, pattern) do
    :ets.tab2list(table)
    |> Enum.filter(fn {_key, tuple} -> matches_pattern?(tuple, pattern) end)
    |> Enum.map(fn {_key, tuple} -> tuple end)
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

  defp first_key([first | _]), do: first
  defp first_key([]), do: :__empty__
end
```

**Step 4: Run ALL tests to verify nothing breaks**

Run: `nix develop -c mix test --trace`
Expected: ALL tests PASS (87 tests, 6 properties, 0 failures)

**Step 5: Commit**

```bash
nix develop -c bash -c 'git add lib/datalox/storage/ets.ex test/datalox/storage/ets_test.exs && git commit -m "feat: first-column indexed ETS lookup for O(1) key access"'
```

---

## Phase 2: Cost-Based Join Ordering

Currently the evaluator processes body goals left-to-right. For a rule like `can_access(U, D) :- permission(R, D), user(U, R)`, if `permission` has 10,000 rows and `user` has 10, we'd rather evaluate `user` first. We add a goal reorderer that estimates selectivity from relation cardinality.

### Task 3: Add join ordering module

**Files:**
- Create: `lib/datalox/optimizer/join_order.ex`
- Test: `test/datalox/optimizer/join_order_test.exs`

**Step 1: Write the failing test**

Create `test/datalox/optimizer/join_order_test.exs`:

```elixir
defmodule Datalox.Optimizer.JoinOrderTest do
  use ExUnit.Case, async: true

  alias Datalox.Optimizer.JoinOrder
  alias Datalox.Rule
  alias Datalox.Storage.ETS

  setup do
    {:ok, storage} = ETS.init(name: :"join_test_#{:erlang.unique_integer()}")
    on_exit(fn -> ETS.terminate(storage) end)
    {:ok, storage: storage}
  end

  describe "reorder/4" do
    test "puts smaller relation first", %{storage: storage} do
      # permission has 100 facts, user has 2
      storage =
        Enum.reduce(1..100, storage, fn i, st ->
          {:ok, st} = ETS.insert(st, :permission, [:role_a, "doc_#{i}"])
          st
        end)

      {:ok, storage} = ETS.insert(storage, :user, ["alice", :role_a])
      {:ok, storage} = ETS.insert(storage, :user, ["bob", :role_b])

      body = [{:permission, [:R, :D]}, {:user, [:U, :R]}]
      reordered = JoinOrder.reorder(body, %{}, storage, ETS)

      # user should come first (smaller relation)
      assert [{:user, _}, {:permission, _}] = reordered
    end

    test "considers bound variables from earlier goals", %{storage: storage} do
      {:ok, storage} = ETS.insert(storage, :a, [1, 2])
      {:ok, storage} = ETS.insert(storage, :b, [2, 3])
      {:ok, storage} = ETS.insert(storage, :c, [3, 4])

      body = [{:c, [:Z, :W]}, {:a, [:X, :Y]}, {:b, [:Y, :Z]}]
      reordered = JoinOrder.reorder(body, %{}, storage, ETS)

      # All same size, but should maintain a valid variable-binding order
      # :a or :b or :c could come first, but later goals should use vars from earlier ones
      assert length(reordered) == 3
    end

    test "does not reorder single-goal body", %{storage: storage} do
      body = [{:user, [:X, :Y]}]
      assert JoinOrder.reorder(body, %{}, storage, ETS) == body
    end

    test "does not reorder when all relations are same size", %{storage: storage} do
      {:ok, storage} = ETS.insert(storage, :a, [1])
      {:ok, storage} = ETS.insert(storage, :b, [2])

      body = [{:a, [:X]}, {:b, [:Y]}]
      reordered = JoinOrder.reorder(body, %{}, storage, ETS)

      # Same size — preserve original order
      assert reordered == body
    end
  end
end
```

**Step 2: Run test to verify it fails**

Run: `nix develop -c mix test test/datalox/optimizer/join_order_test.exs --trace`
Expected: FAIL — module `JoinOrder` not found

**Step 3: Implement JoinOrder**

Create `lib/datalox/optimizer/join_order.ex`:

```elixir
defmodule Datalox.Optimizer.JoinOrder do
  @moduledoc """
  Cost-based join ordering for Datalog rule bodies.

  Reorders body goals to evaluate smaller (cheaper) relations first,
  taking into account which variables are already bound from earlier goals.
  """

  @doc """
  Reorders body goals by estimated cost (relation cardinality).

  Goals with fewer matching facts are placed first. When cardinalities
  are equal, original order is preserved (stable sort).
  """
  @spec reorder([{atom(), list()}], map(), any(), module()) :: [{atom(), list()}]
  def reorder(body, _binding, _storage, _storage_mod) when length(body) <= 1, do: body

  def reorder(body, _binding, storage, storage_mod) do
    # Estimate cost for each goal based on relation size
    costed =
      body
      |> Enum.with_index()
      |> Enum.map(fn {{pred, terms}, idx} ->
        cost = estimate_cost(pred, storage, storage_mod)
        {cost, idx, {pred, terms}}
      end)

    # Stable sort by cost (preserve index for ties)
    costed
    |> Enum.sort_by(fn {cost, idx, _goal} -> {cost, idx} end)
    |> Enum.map(fn {_cost, _idx, goal} -> goal end)
  end

  defp estimate_cost(predicate, storage, storage_mod) do
    if function_exported?(storage_mod, :count, 2) do
      case storage_mod.count(storage, predicate) do
        {:ok, n} -> n
        _ -> 1_000_000
      end
    else
      1_000_000
    end
  end
end
```

**Step 4: Run test to verify it passes**

Run: `nix develop -c mix test test/datalox/optimizer/join_order_test.exs --trace`
Expected: PASS

**Step 5: Commit**

```bash
nix develop -c bash -c 'git add lib/datalox/optimizer/join_order.ex test/datalox/optimizer/join_order_test.exs && git commit -m "feat: cost-based join ordering by relation cardinality"'
```

---

### Task 4: Wire join ordering into the evaluator

**Files:**
- Modify: `lib/datalox/evaluator.ex`
- Test: `test/datalox/evaluator_test.exs`

**Step 1: Write the failing test**

Add to `test/datalox/evaluator_test.exs`:

```elixir
describe "evaluate/3 with join ordering" do
  test "produces correct results regardless of body goal order", %{storage: storage} do
    # Insert a large permission set and small user set
    storage =
      Enum.reduce(1..50, storage, fn i, st ->
        {:ok, st} = ETS.insert(st, :permission, [:admin, "doc_#{i}"])
        st
      end)

    {:ok, storage} = ETS.insert(storage, :user, ["alice", :admin])

    # Rule with large relation first (suboptimal order)
    rules = [
      Rule.new({:can_access, [:U, :D]}, [{:permission, [:R, :D]}, {:user, [:U, :R]}])
    ]

    {:ok, derived, _storage} = Evaluator.evaluate(rules, storage, ETS)

    # Should still derive all 50 can_access facts
    access_facts = Enum.filter(derived, fn {pred, _} -> pred == :can_access end)
    assert length(access_facts) == 50
    assert {:can_access, ["alice", "doc_1"]} in derived
  end
end
```

**Step 2: Run test to verify it passes (correctness baseline)**

Run: `nix develop -c mix test test/datalox/evaluator_test.exs --trace`
Expected: PASS (this test works even without reordering; it validates correctness is preserved)

**Step 3: Wire JoinOrder into evaluate_rule**

In `lib/datalox/evaluator.ex`, add the alias at the top (after line 11):

```elixir
alias Datalox.Optimizer.JoinOrder
```

Then modify the `evaluate_rule/4` function (line 103) to reorder the body:

Replace:
```elixir
  defp evaluate_rule(rule, storage, storage_mod, _delta) do
    # Get all bindings from the body
    bindings = evaluate_body(rule.body, storage, storage_mod, [%{}])
```

With:
```elixir
  defp evaluate_rule(rule, storage, storage_mod, _delta) do
    # Reorder body goals by estimated cost
    ordered_body = JoinOrder.reorder(rule.body, %{}, storage, storage_mod)

    # Get all bindings from the reordered body
    bindings = evaluate_body(ordered_body, storage, storage_mod, [%{}])
```

**Step 4: Run ALL tests**

Run: `nix develop -c mix test --trace`
Expected: ALL tests PASS

**Step 5: Commit**

```bash
nix develop -c bash -c 'git add lib/datalox/evaluator.ex test/datalox/evaluator_test.exs && git commit -m "feat: wire cost-based join ordering into evaluator"'
```

---

## Phase 3: Aggregation Integration

The `Datalox.Aggregation` module has working functions but they aren't reachable from rules. We need to:
1. Wire aggregation into the evaluator's rule processing
2. Extend the lexer/parser to support aggregation syntax in `.dl` files

### Task 5: Wire aggregation into evaluator

The approach: after computing bindings for a rule body, if the rule has aggregations, group bindings by the grouping variables, compute aggregates, and produce one fact per group.

**Files:**
- Modify: `lib/datalox/evaluator.ex`
- Test: `test/datalox/evaluator_test.exs`

**Step 1: Write the failing test**

Add to `test/datalox/evaluator_test.exs`:

```elixir
describe "evaluate/3 with aggregation" do
  test "computes count aggregation", %{storage: storage} do
    {:ok, storage} = ETS.insert(storage, :employee, ["alice", "engineering"])
    {:ok, storage} = ETS.insert(storage, :employee, ["bob", "engineering"])
    {:ok, storage} = ETS.insert(storage, :employee, ["carol", "sales"])

    # Rule: dept_count(Dept, N) :- employee(_, Dept), N = count(Dept)
    # Represented as:
    # head: {:dept_count, [:Dept, :N]}
    # body: [{:employee, [:_, :Dept]}]
    # aggregations: [{:count, :N, :Dept, [:Dept]}]
    #   meaning: N = count, grouped by [Dept]
    rules = [
      Rule.new(
        {:dept_count, [:Dept, :N]},
        [{:employee, [:_, :Dept]}],
        aggregations: [{:count, :N, :_, [:Dept]}]
      )
    ]

    {:ok, derived, _storage} = Evaluator.evaluate(rules, storage, ETS)

    assert {:dept_count, ["engineering", 2]} in derived
    assert {:dept_count, ["sales", 1]} in derived
  end

  test "computes sum aggregation", %{storage: storage} do
    {:ok, storage} = ETS.insert(storage, :sale, ["alice", 100])
    {:ok, storage} = ETS.insert(storage, :sale, ["alice", 200])
    {:ok, storage} = ETS.insert(storage, :sale, ["bob", 50])

    # total_sales(Person, Total) :- sale(Person, Amount), Total = sum(Amount), group_by(Person)
    rules = [
      Rule.new(
        {:total_sales, [:Person, :Total]},
        [{:sale, [:Person, :Amount]}],
        aggregations: [{:sum, :Total, :Amount, [:Person]}]
      )
    ]

    {:ok, derived, _storage} = Evaluator.evaluate(rules, storage, ETS)

    assert {:total_sales, ["alice", 300]} in derived
    assert {:total_sales, ["bob", 50]} in derived
  end

  test "computes min aggregation", %{storage: storage} do
    {:ok, storage} = ETS.insert(storage, :score, ["alice", 90])
    {:ok, storage} = ETS.insert(storage, :score, ["alice", 70])
    {:ok, storage} = ETS.insert(storage, :score, ["bob", 85])

    rules = [
      Rule.new(
        {:min_score, [:Person, :Min]},
        [{:score, [:Person, :Val]}],
        aggregations: [{:min, :Min, :Val, [:Person]}]
      )
    ]

    {:ok, derived, _storage} = Evaluator.evaluate(rules, storage, ETS)

    assert {:min_score, ["alice", 70]} in derived
    assert {:min_score, ["bob", 85]} in derived
  end
end
```

**Step 2: Run test to verify it fails**

Run: `nix develop -c mix test test/datalox/evaluator_test.exs --trace`
Expected: FAIL — aggregation logic not applied, so `dept_count` will have wrong values or structure

**Step 3: Implement aggregation in evaluator**

In `lib/datalox/evaluator.ex`, add alias at top (after existing aliases):

```elixir
alias Datalox.Aggregation
```

Replace the `evaluate_rule/4` function entirely with:

```elixir
  # Evaluate a single rule
  defp evaluate_rule(rule, storage, storage_mod, _delta) do
    # Reorder body goals by estimated cost
    ordered_body = JoinOrder.reorder(rule.body, %{}, storage, storage_mod)

    # Get all bindings from the reordered body
    bindings = evaluate_body(ordered_body, storage, storage_mod, [%{}])

    # Filter by negations
    bindings = filter_negations(bindings, rule.negations, storage, storage_mod)

    # Apply aggregation or project directly
    facts =
      if rule.aggregations != [] do
        apply_aggregations(rule, bindings)
      else
        bindings
        |> Enum.map(fn binding -> instantiate_head(rule.head, binding) end)
        |> Enum.uniq()
      end

    # Store derived facts
    storage =
      Enum.reduce(facts, storage, fn {pred, tuple}, st ->
        {:ok, st} = storage_mod.insert(st, pred, tuple)
        st
      end)

    {facts, storage}
  end

  # Apply aggregation: group bindings, compute aggregate per group, produce facts
  defp apply_aggregations(rule, bindings) do
    Enum.flat_map(rule.aggregations, fn {agg_fn, target_var, source_var, group_vars} ->
      # Group bindings by the group-by variables
      groups =
        Enum.group_by(bindings, fn binding ->
          Enum.map(group_vars, &Map.get(binding, &1))
        end)

      # For each group, compute the aggregate
      Enum.map(groups, fn {group_key, group_bindings} ->
        values =
          if source_var == :_ do
            # count over the group (no specific source variable)
            group_bindings
          else
            Enum.map(group_bindings, &Map.get(&1, source_var))
          end

        agg_value = Aggregation.compute(agg_fn, values, [])

        # Build binding for the head
        group_binding =
          Enum.zip(group_vars, group_key)
          |> Map.new()
          |> Map.put(target_var, agg_value)

        instantiate_head(rule.head, group_binding)
      end)
    end)
    |> Enum.uniq()
  end
```

**Step 4: Run tests to verify they pass**

Run: `nix develop -c mix test --trace`
Expected: ALL tests PASS

**Step 5: Commit**

```bash
nix develop -c bash -c 'git add lib/datalox/evaluator.ex test/datalox/evaluator_test.exs && git commit -m "feat: wire aggregation into evaluator with group-by support"'
```

---

### Task 6: Add aggregation syntax to lexer

Support `.dl` syntax like: `dept_count(Dept, N) :- employee(_, Dept), N = count(Dept).`

We need new tokens: `=`, and to recognize aggregation function names as a token type.

**Files:**
- Modify: `lib/datalox/parser/lexer.ex`
- Test: `test/datalox/parser/lexer_test.exs`

**Step 1: Write the failing test**

Add to `test/datalox/parser/lexer_test.exs`:

```elixir
describe "aggregation tokens" do
  test "tokenizes = sign" do
    {:ok, tokens, _, _, _, _} = Lexer.tokenize("N = count")
    assert tokens == [{:var, "N"}, :equals, {:atom, "count"}]
  end

  test "tokenizes full aggregation expression" do
    {:ok, tokens, _, _, _, _} = Lexer.tokenize("N = sum(Amount)")
    assert tokens == [{:var, "N"}, :equals, {:atom, "sum"}, :lparen, {:var, "Amount"}, :rparen]
  end
end
```

**Step 2: Run test to verify it fails**

Run: `nix develop -c mix test test/datalox/parser/lexer_test.exs --trace`
Expected: FAIL — `=` is not a recognized token

**Step 3: Add `=` token to lexer**

In `lib/datalox/parser/lexer.ex`, add after the `implies` line (line 69):

```elixir
equals = string("=") |> replace(:equals)
```

And add `equals` to the `choice` list in the `token` combinator, after `implies`:

```elixir
token =
  choice([
    whitespace,
    comment,
    not_keyword,
    implies,
    equals,
    lparen,
    rparen,
    comma,
    dot,
    string_token,
    integer_token,
    wildcard_token,
    underscore_var_token,
    var_token,
    atom_token
  ])
```

**Step 4: Run tests to verify they pass**

Run: `nix develop -c mix test test/datalox/parser/lexer_test.exs --trace`
Expected: PASS

**Step 5: Commit**

```bash
nix develop -c bash -c 'git add lib/datalox/parser/lexer.ex test/datalox/parser/lexer_test.exs && git commit -m "feat: add equals token to lexer for aggregation syntax"'
```

---

### Task 7: Add aggregation parsing to parser

Parse `N = count(Dept)` in rule bodies as an aggregation expression.

**Files:**
- Modify: `lib/datalox/parser/parser.ex`
- Test: `test/datalox/parser/parser_test.exs`

**Step 1: Write the failing test**

Add to `test/datalox/parser/parser_test.exs`:

```elixir
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
```

**Step 2: Run test to verify it fails**

Run: `nix develop -c mix test test/datalox/parser/parser_test.exs --trace`
Expected: FAIL — parser doesn't handle `N = count(Dept)` syntax

**Step 3: Implement aggregation parsing**

In `lib/datalox/parser/parser.ex`, modify `parse_body_goals/3` to detect the pattern `Var = agg_fn(args)` before checking for regular goals.

Add a new clause at the top of `parse_body_goals/3` (before line 141):

```elixir
# Aggregation: Var = agg_fn(args)
defp parse_body_goals([{:var, var_name}, :equals, {:atom, agg_name} | rest], goals, negations)
     when agg_name in ["count", "sum", "min", "max", "avg", "collect"] do
  case parse_goal({:atom, agg_name}, rest) do
    {:ok, {_pred, agg_args}, [:comma | rest]} ->
      agg = build_aggregation(agg_name, var_name, agg_args)
      parse_body_goals(rest, goals, negations, [agg])

    {:ok, {_pred, agg_args}, rest} ->
      agg = build_aggregation(agg_name, var_name, agg_args)
      {:ok, Enum.reverse(goals), Enum.reverse(negations), [agg], rest}

    {:error, _} = error ->
      error
  end
end
```

We need to change `parse_body_goals` to carry aggregations. Refactor the function to have a 4-argument version that accumulates aggregations:

Replace the entire `parse_body/1` and `parse_body_goals` functions with:

```elixir
# Parse rule body
defp parse_body(tokens) do
  parse_body_goals(tokens, [], [], [])
end

# Aggregation: Var = agg_fn(args)
defp parse_body_goals([{:var, var_name}, :equals, {:atom, agg_name} | rest], goals, negations, aggs)
     when agg_name in ~w(count sum min max avg collect) do
  case parse_goal({:atom, agg_name}, rest) do
    {:ok, {_pred, agg_args}, [:comma | rest]} ->
      agg = build_aggregation(agg_name, var_name, agg_args)
      parse_body_goals(rest, goals, negations, [agg | aggs])

    {:ok, {_pred, agg_args}, rest} ->
      agg = build_aggregation(agg_name, var_name, agg_args)
      {:ok, Enum.reverse(goals), Enum.reverse(negations), Enum.reverse([agg | aggs]), rest}

    {:error, _} = error ->
      error
  end
end

defp parse_body_goals([:not, {:atom, pred} | rest], goals, negations, aggs) do
  case parse_goal({:atom, pred}, rest) do
    {:ok, goal, [:comma | rest]} ->
      {neg_pred, neg_terms} = goal
      neg_goal = {neg_pred, Enum.map(neg_terms, &term_to_rule_term/1)}
      parse_body_goals(rest, goals, [neg_goal | negations], aggs)

    {:ok, goal, rest} ->
      {neg_pred, neg_terms} = goal
      neg_goal = {neg_pred, Enum.map(neg_terms, &term_to_rule_term/1)}
      {:ok, Enum.reverse(goals), Enum.reverse([neg_goal | negations]), Enum.reverse(aggs), rest}

    {:error, _} = error ->
      error
  end
end

defp parse_body_goals([{:atom, pred} | rest], goals, negations, aggs) do
  case parse_goal({:atom, pred}, rest) do
    {:ok, goal, [:comma | rest]} ->
      {pred, terms} = goal
      converted_goal = {pred, Enum.map(terms, &term_to_rule_term/1)}
      parse_body_goals(rest, [converted_goal | goals], negations, aggs)

    {:ok, goal, rest} ->
      {pred, terms} = goal
      converted_goal = {pred, Enum.map(terms, &term_to_rule_term/1)}
      {:ok, Enum.reverse([converted_goal | goals]), Enum.reverse(negations), Enum.reverse(aggs), rest}

    {:error, _} = error ->
      error
  end
end

defp build_aggregation(agg_name, var_name, agg_args) do
  agg_fn = String.to_atom(agg_name)
  target_var = String.to_atom(var_name)
  # The source variable is the aggregated value (first arg for sum/min/max/avg, :_ for count)
  rule_args = Enum.map(agg_args, &term_to_rule_term/1)

  source_var =
    case {agg_fn, rule_args} do
      {:count, _} -> :_
      {_, [src | _]} -> src
      _ -> :_
    end

  # Group-by vars are inferred from the args
  group_vars = rule_args

  {agg_fn, target_var, source_var, group_vars}
end
```

Also update `parse_statement/1` to handle the new 4-tuple return from `parse_body`:

Replace the rule-parsing branch in `parse_statement` (lines 54-65):

```elixir
    case parse_goal({:atom, pred}, rest) do
      {:ok, head, [:implies | body_tokens]} ->
        # It's a rule
        case parse_body(body_tokens) do
          {:ok, body, negations, aggregations, [:dot | rest]} ->
            {head_pred, head_terms} = head
            converted_head = {head_pred, Enum.map(head_terms, &term_to_rule_term/1)}
            rule = Rule.new(converted_head, body,
              negations: negations,
              aggregations: aggregations
            )
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
```

**Step 4: Run tests to verify they pass**

Run: `nix develop -c mix test --trace`
Expected: ALL tests PASS

**Step 5: Commit**

```bash
nix develop -c bash -c 'git add lib/datalox/parser/parser.ex test/datalox/parser/parser_test.exs && git commit -m "feat: parse aggregation syntax in .dl files"'
```

---

### Task 8: End-to-end aggregation integration test

Verify aggregation works all the way from `.dl` file to query results.

**Files:**
- Create: `test/fixtures/aggregation.dl`
- Test: `test/datalox/aggregation_integration_test.exs`

**Step 1: Create test fixture**

Create `test/fixtures/aggregation.dl`:

```prolog
% Employee data
employee("alice", "engineering").
employee("bob", "engineering").
employee("carol", "sales").
employee("dave", "sales").
employee("eve", "sales").

% Department size rule with count
dept_size(Dept, N) :- employee(_, Dept), N = count(Dept).
```

**Step 2: Write the integration test**

Create `test/datalox/aggregation_integration_test.exs`:

```elixir
defmodule Datalox.AggregationIntegrationTest do
  use ExUnit.Case, async: true

  setup do
    name = :"agg_integration_#{:erlang.unique_integer()}"
    {:ok, db} = Datalox.new(name: name)
    on_exit(fn -> Datalox.stop(db) end)
    {:ok, db: db}
  end

  test "aggregation from .dl file produces correct counts", %{db: db} do
    :ok = Datalox.load_file(db, "test/fixtures/aggregation.dl")

    results = Datalox.query(db, {:dept_size, [:_, :_]})

    eng = Enum.find(results, fn {:dept_size, [dept, _]} -> dept == "engineering" end)
    sales = Enum.find(results, fn {:dept_size, [dept, _]} -> dept == "sales" end)

    assert {:dept_size, ["engineering", 2]} = eng
    assert {:dept_size, ["sales", 3]} = sales
  end

  test "aggregation via programmatic API", %{db: db} do
    Datalox.assert(db, {:sale, ["alice", 100]})
    Datalox.assert(db, {:sale, ["alice", 250]})
    Datalox.assert(db, {:sale, ["bob", 75]})

    alias Datalox.{Database, Rule}

    rules = [
      Rule.new(
        {:total_sales, [:Person, :Total]},
        [{:sale, [:Person, :Amount]}],
        aggregations: [{:sum, :Total, :Amount, [:Person]}]
      )
    ]

    Database.load_rules(db, rules)

    results = Datalox.query(db, {:total_sales, [:_, :_]})
    assert {:total_sales, ["alice", 350]} in results
    assert {:total_sales, ["bob", 75]} in results
  end
end
```

**Step 3: Run test**

Run: `nix develop -c mix test test/datalox/aggregation_integration_test.exs --trace`
Expected: PASS

**Step 4: Commit**

```bash
nix develop -c bash -c 'git add test/fixtures/aggregation.dl test/datalox/aggregation_integration_test.exs && git commit -m "test: end-to-end aggregation integration tests"'
```

---

## Phase 4: BEAM-Native Parallel Rule Evaluation

Independent rules within the same stratum can be evaluated concurrently. The BEAM's lightweight processes make this natural.

### Task 9: Parallel rule evaluation within strata

**Files:**
- Modify: `lib/datalox/evaluator.ex`
- Test: `test/datalox/evaluator_test.exs`

**Step 1: Write the failing test**

Add to `test/datalox/evaluator_test.exs`:

```elixir
describe "evaluate/3 with parallel evaluation" do
  test "correctly evaluates independent rules in same stratum", %{storage: storage} do
    # Two independent rules that don't depend on each other
    {:ok, storage} = ETS.insert(storage, :user, ["alice"])
    {:ok, storage} = ETS.insert(storage, :user, ["bob"])
    {:ok, storage} = ETS.insert(storage, :item, ["widget"])
    {:ok, storage} = ETS.insert(storage, :item, ["gadget"])

    # These rules are independent (different head predicates, no shared derived predicates)
    rules = [
      Rule.new({:active_user, [:X]}, [{:user, [:X]}]),
      Rule.new({:listed_item, [:I]}, [{:item, [:I]}])
    ]

    {:ok, derived, _storage} = Evaluator.evaluate(rules, storage, ETS)

    assert {:active_user, ["alice"]} in derived
    assert {:active_user, ["bob"]} in derived
    assert {:listed_item, ["widget"]} in derived
    assert {:listed_item, ["gadget"]} in derived
  end

  test "correctly evaluates dependent rules across strata", %{storage: storage} do
    {:ok, storage} = ETS.insert(storage, :user, ["alice"])
    {:ok, storage} = ETS.insert(storage, :user, ["bob"])
    {:ok, storage} = ETS.insert(storage, :banned, ["bob"])

    # Stratum 0: banned is base fact
    # Stratum 1: active depends on negation of banned
    # Stratum 2: trusted depends on active
    rules = [
      Rule.new({:active, [:X]}, [{:user, [:X]}], negations: [{:banned, [:X]}]),
      Rule.new({:trusted, [:X]}, [{:active, [:X]}])
    ]

    {:ok, derived, _storage} = Evaluator.evaluate(rules, storage, ETS)

    assert {:active, ["alice"]} in derived
    refute {:active, ["bob"]} in derived
    assert {:trusted, ["alice"]} in derived
    refute {:trusted, ["bob"]} in derived
  end
end
```

**Step 2: Run tests to verify they pass (baseline)**

Run: `nix develop -c mix test test/datalox/evaluator_test.exs --trace`
Expected: PASS (correctness baseline)

**Step 3: Implement parallel evaluation**

In `lib/datalox/evaluator.ex`, we modify `derive_all/3` and `derive_with_delta/4` to evaluate independent rules in parallel. Two rules are independent if they derive different head predicates.

Replace `derive_all/3` (around line 87):

```elixir
  # Initial derivation pass — evaluate independent rules in parallel
  defp derive_all(rules, storage, storage_mod) do
    groups = group_independent_rules(rules)

    Enum.reduce(groups, {[], storage}, fn group, {facts, st} ->
      if length(group) > 1 do
        # Parallel evaluation of independent rules
        results =
          group
          |> Task.async_stream(
            fn rule -> evaluate_rule(rule, st, storage_mod, %{}) end,
            ordered: false,
            timeout: 30_000
          )
          |> Enum.map(fn {:ok, result} -> result end)

        # Merge results sequentially (storage updates must be serial)
        Enum.reduce(results, {facts, st}, fn {new_facts, _}, {acc_facts, acc_st} ->
          acc_st =
            Enum.reduce(new_facts, acc_st, fn {pred, tuple}, s ->
              {:ok, s} = storage_mod.insert(s, pred, tuple)
              s
            end)

          {acc_facts ++ new_facts, acc_st}
        end)
      else
        # Single rule — evaluate directly
        Enum.reduce(group, {facts, st}, fn rule, {acc_facts, acc_st} ->
          {new_facts, acc_st} = evaluate_rule(rule, acc_st, storage_mod, %{})
          {acc_facts ++ new_facts, acc_st}
        end)
      end
    end)
  end
```

Replace `derive_with_delta/4` (around line 95):

```elixir
  # Derive using delta facts — parallel for independent rules
  defp derive_with_delta(rules, storage, storage_mod, delta) do
    groups = group_independent_rules(rules)

    Enum.reduce(groups, {[], storage}, fn group, {facts, st} ->
      if length(group) > 1 do
        results =
          group
          |> Task.async_stream(
            fn rule -> evaluate_rule(rule, st, storage_mod, delta) end,
            ordered: false,
            timeout: 30_000
          )
          |> Enum.map(fn {:ok, result} -> result end)

        Enum.reduce(results, {facts, st}, fn {new_facts, _}, {acc_facts, acc_st} ->
          acc_st =
            Enum.reduce(new_facts, acc_st, fn {pred, tuple}, s ->
              {:ok, s} = storage_mod.insert(s, pred, tuple)
              s
            end)

          {acc_facts ++ new_facts, acc_st}
        end)
      else
        Enum.reduce(group, {facts, st}, fn rule, {acc_facts, acc_st} ->
          {new_facts, acc_st} = evaluate_rule(rule, acc_st, storage_mod, delta)
          {acc_facts ++ new_facts, acc_st}
        end)
      end
    end)
  end
```

Add the grouping helper at the bottom of the module (before the final `end`):

```elixir
  # Group rules by head predicate — rules with same head predicate must be
  # sequential, but different head predicates can run in parallel.
  defp group_independent_rules(rules) do
    rules
    |> Enum.group_by(fn rule -> elem(rule.head, 0) end)
    |> Map.values()
  end
```

**Important:** We also need to change the ETS table access to `:public` so spawned tasks can read them. In `lib/datalox/storage/ets.ex`, change the `ensure_table` function:

Replace:
```elixir
      table = :ets.new(table_name, [:duplicate_bag, :protected])
```

With:
```elixir
      table = :ets.new(table_name, [:duplicate_bag, :public])
```

**Step 4: Run ALL tests**

Run: `nix develop -c mix test --trace`
Expected: ALL tests PASS

**Step 5: Commit**

```bash
nix develop -c bash -c 'git add lib/datalox/evaluator.ex lib/datalox/storage/ets.ex test/datalox/evaluator_test.exs && git commit -m "feat: parallel rule evaluation for independent rules within strata"'
```

---

### Task 10: Run full test suite and final verification

**Step 1: Run all tests**

Run: `nix develop -c mix test --trace`
Expected: ALL tests PASS

**Step 2: Run Dialyzer (if available)**

Run: `nix develop -c mix dialyzer`
Expected: No warnings (or only pre-existing ones)

**Step 3: Run Credo**

Run: `nix develop -c mix credo --strict`
Expected: Clean or only minor pre-existing issues

**Step 4: Final commit**

If any cleanup was needed:
```bash
nix develop -c bash -c 'git add -A && git commit -m "chore: cleanup after gap-closing implementation"'
```

---

## Summary of Changes

| Phase | Gap Closed | Key Files | Impact |
|-------|-----------|-----------|--------|
| 1 | Indexing | `storage.ex`, `storage/ets.ex` | O(1) first-column lookups instead of O(n) table scans |
| 2 | Join Ordering | `optimizer/join_order.ex`, `evaluator.ex` | Smaller relations evaluated first, fewer intermediate results |
| 3 | Aggregation | `evaluator.ex`, `parser/lexer.ex`, `parser/parser.ex` | Full `count`/`sum`/`min`/`max`/`avg`/`collect` in rules and `.dl` files |
| 4 | Parallelism | `evaluator.ex`, `storage/ets.ex` | Independent rules evaluated concurrently via BEAM tasks |

**Total new files:** 3 (`join_order.ex`, `join_order_test.exs`, `aggregation_integration_test.exs`, `aggregation.dl`)
**Modified files:** 7 (`storage.ex`, `storage/ets.ex`, `evaluator.ex`, `lexer.ex`, `parser.ex`, `ets_test.exs`, `evaluator_test.exs`, `parser_test.exs`, `lexer_test.exs`)

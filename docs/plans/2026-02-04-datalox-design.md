# Datalox Design Document

A tested, documented Datalog implementation in Elixir for general-purpose rule engines.

## Overview

**Purpose:** General-purpose rule engine supporting authorization, data validation, and business logic derivation.

**Key Features:**
- Stratified negation + aggregation
- Incremental maintenance for large-scale (millions of facts)
- Magic Sets query optimization
- Pluggable storage with ETS default
- Multiple input methods: programmatic API, macro DSL, external files
- Full observability: explain queries, dependency graphs, metrics, change tracking

## Architecture

```
┌─────────────────────────────────────────┐
│  Interface Layer                        │
│  - Macro DSL (defrule, deffact)        │
│  - File parser (.dl files)             │
│  - Pattern-matching query API          │
└─────────────────────────────────────────┘
                    │
┌─────────────────────────────────────────┐
│  Core Engine                            │
│  - Rule compiler & stratification       │
│  - Incremental evaluator               │
│  - Index manager                        │
│  - Aggregation engine                   │
│  - Magic Sets optimizer                 │
└─────────────────────────────────────────┘
                    │
┌─────────────────────────────────────────┐
│  Storage Layer                          │
│  - Storage behaviour                    │
│  - ETS adapter (default)               │
│  - Index structures (per-column)       │
└─────────────────────────────────────────┘
```

## Data Representation

### Internal Representation

```elixir
# Fact
{:user, ["alice", :admin, "engineering"]}

# Rule
%Datalox.Rule{
  head: {:can_access, [:User, :Resource]},
  body: [
    {:user, [:User, :Role, :Dept]},
    {:resource, [:Resource, :Dept]},
    {:permission, [:Role, :access]}
  ],
  negations: [],
  aggregations: [],
  guards: []
}
```

### Macro DSL

```elixir
defmodule MyRules do
  use Datalox.DSL

  deffact user("alice", :admin, "engineering")
  deffact user("bob", :viewer, "sales")

  defrule can_access(user, resource) do
    user(user, role, dept) and
    resource(resource, dept) and
    permission(role, :access)
  end

  defrule inactive(user) do
    user(user, _, _) and
    not login(user, _)
  end

  defrule high_spender(user) do
    user(user, _, _) and
    sum(amount, purchase(user, _, amount)) > 1000
  end
end
```

### External File Format (.dl)

```prolog
% Facts
user("alice", admin, "engineering").
user("bob", viewer, "sales").

% Rules
can_access(User, Resource) :-
  user(User, Role, Dept),
  resource(Resource, Dept),
  permission(Role, access).
```

## Query Optimization: Magic Sets

Transform rules to be goal-directed, deriving only facts relevant to the query.

```
# Original rule
ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z).

# Query: ancestor("alice", Who)?

# Magic-transformed:
magic_ancestor("alice").
magic_ancestor(Y) :- magic_ancestor(X), parent(X, Y).
ancestor(X, Z) :- magic_ancestor(X), parent(X, Z).
ancestor(X, Z) :- magic_ancestor(X), parent(X, Y), ancestor(Y, Z).
```

Integration with incremental maintenance:
- Magic sets for ad-hoc queries
- Incremental maintenance for standing queries / materialized views
- User declares which predicates to materialize vs compute on-demand

## Storage Layer

### Behaviour

```elixir
defmodule Datalox.Storage do
  @callback init(opts :: keyword()) :: {:ok, state} | {:error, term()}
  @callback insert(state, predicate, tuple) :: {:ok, state} | {:error, term()}
  @callback delete(state, predicate, tuple) :: {:ok, state} | {:error, term()}
  @callback lookup(state, predicate, pattern) :: {:ok, [tuple]} | {:error, term()}
  @callback all(state, predicate) :: {:ok, [tuple]} | {:error, term()}
end
```

### Indexing Strategy

- Automatic indexes on first column (primary)
- User-declared secondary indexes: `Datalox.create_index(db, :purchase, [0, 2])`
- Hash index for equality lookups
- Composite index for multi-column joins
- Index selection during query planning based on selectivity

### Memory Management

- Configurable limits per predicate
- LRU eviction for derived facts (can be recomputed)
- Base facts never auto-evicted

## Incremental Evaluation

### Dependency Tracking

```elixir
%Datalox.DependencyGraph{
  predicate_to_rules: %{can_access: [rule_1, rule_2]},
  rule_to_predicates: %{rule_1: [:user, :resource, :permission]},
  strata: [[:user, :resource], [:inactive], [:can_access]]
}
```

### Update Algorithm

**On fact insertion:**
1. Add fact to base storage
2. Find all rules that reference this predicate
3. For each affected rule (in stratification order):
   - Compute new derivations using semi-naive delta
   - Add new derived facts
   - Propagate to dependent rules

**On fact deletion:**
1. Remove fact from base storage
2. Find derived facts that depended on it
3. Re-derive or delete (truth maintenance)
4. Propagate deletions through dependency chain

### Batching

```elixir
Datalox.transaction(db, fn ->
  Datalox.assert(db, {:user, ["charlie", :admin, "ops"]})
  Datalox.assert(db, {:user, ["dana", :viewer, "ops"]})
  Datalox.retract(db, {:user, ["bob", :viewer, "sales"]})
end)
# Single incremental update for all changes
```

## Aggregation

### Supported Aggregates

- `count(Var, Goal)` - Count matching bindings
- `sum(Var, Goal)` - Sum numeric values
- `min(Var, Goal)` / `max(Var, Goal)` - Extrema
- `avg(Var, Goal)` - Average
- `collect(Var, Goal)` - Gather into list

### Incremental Aggregation

Aggregate maintenance tables track current values per group:

```elixir
%AggregateState{
  predicate: :purchase,
  group_by: [0],
  aggregate: :sum,
  column: 2,
  current_values: %{"alice" => 1500, "bob" => 800}
}
```

- On insert: Add value to group's aggregate
- On delete: Subtract value from group's aggregate
- Re-evaluate dependent rules only when aggregate crosses thresholds

## Observability

### Query Explanation

```elixir
Datalox.explain(db, {:can_access, ["alice", :_, "doc_123"]})

%Explanation{
  fact: {:can_access, ["alice", :read, "doc_123"]},
  rule: :can_access_rule_1,
  derivation: [
    {:base, {:user, ["alice", :admin, "engineering"]}},
    {:base, {:resource, ["doc_123", "engineering"]}},
    {:base, {:permission, [:admin, :read]}}
  ]
}
```

### Dependency Graph Inspection

```elixir
Datalox.Inspect.dependency_graph(db)
Datalox.Inspect.why_stratified?(db, :my_rule)
Datalox.Inspect.affected_by(db, {:user, ["alice", _, _]})
```

### Performance Metrics

```elixir
Datalox.Metrics.query_stats(db)
%{
  total_queries: 15420,
  avg_latency_us: 245,
  cache_hit_rate: 0.89,
  facts: %{base: 1_250_000, derived: 3_420_000},
  index_usage: %{user_0: 8521, purchase_0_2: 3200}
}
```

### Change Tracking

```elixir
Datalox.subscribe(db, {:can_access, ["alice", :_, :_]})
# Receive: {:datalox, :asserted, {:can_access, ["alice", :write, "doc_456"]}}
# Receive: {:datalox, :retracted, {:can_access, ["alice", :read, "doc_123"]}}
```

## Validation & Errors

### Compile-time Checks

- Stratification violations (circular negation)
- Undefined predicates with suggestions
- Arity mismatches
- Unsafe variables (only in negation)

### Error Messages

```elixir
** (Datalox.StratificationError) Circular negation detected

   bad -> not bar -> not bad

   Rules with negation cannot form cycles. Consider:
   - Restructuring rules to break the cycle
   - Using positive conditions instead

   at my_rules.ex:12
```

## Public API

### Database Lifecycle

```elixir
{:ok, db} = Datalox.new(name: :my_db, storage: Datalox.Storage.ETS)
Datalox.load_rules(db, MyRules)
Datalox.load_file(db, "rules/access_control.dl")
Datalox.stop(db)
```

### Fact Manipulation

```elixir
Datalox.assert(db, {:user, ["alice", :admin, "engineering"]})
Datalox.retract(db, {:user, ["alice", :admin, "engineering"]})
Datalox.assert_all(db, [...])
Datalox.transaction(db, fn -> ... end)
```

### Querying

```elixir
Datalox.query(db, {:can_access, ["alice", :_, :_]})
Datalox.query(db, {:can_access, ["alice", :action, :resource]})
Datalox.query_one(db, {:user, ["alice", :role, :_]})
Datalox.exists?(db, {:can_access, ["alice", :_, "doc_1"]})
```

## Project Structure

```
datalox/
├── lib/
│   └── datalox/
│       ├── database.ex           # GenServer, main coordinator
│       ├── rule.ex               # Rule struct & compilation
│       ├── evaluator.ex          # Semi-naive evaluation
│       ├── incremental.ex        # Delta maintenance
│       ├── aggregation.ex        # Aggregate computation
│       ├── optimizer/
│       │   ├── magic_sets.ex     # Magic sets transformation
│       │   ├── stratifier.ex     # Stratification analysis
│       │   └── index_advisor.ex  # Index recommendations
│       ├── storage/
│       │   ├── behaviour.ex      # Storage behaviour
│       │   └── ets.ex            # ETS adapter
│       ├── parser/
│       │   ├── lexer.ex          # Tokenizer for .dl files
│       │   └── parser.ex         # Parser for .dl files
│       ├── dsl.ex                # Macro DSL (defrule, deffact)
│       ├── validation.ex         # Compile-time checks
│       ├── explain.ex            # Query explanation
│       ├── metrics.ex            # Performance tracking
│       └── errors.ex             # Custom error types
├── test/
├── guides/
├── mix.exs
└── README.md
```

## Dependencies

- `nimble_parsec` - For .dl file parsing
- `telemetry` - For metrics integration
- `stream_data` - Test only, property-based tests

## Testing Strategy

- Unit tests per module
- Property-based tests (StreamData):
  - Query results match naive bottom-up evaluation
  - Incremental updates produce same results as full recomputation
  - Stratification is deterministic
- Benchmark suite for 1M+ facts
- Doctests for all public functions
- CI: `mix test`, `mix dialyzer`, `mix credo --strict`

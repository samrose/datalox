# Datalox

A Datalog implementation in Elixir for building rule engines. Supports stratified negation, aggregation, arithmetic guards, incremental maintenance, Magic Sets query optimization, and pluggable storage backends.

## Installation

Add `datalox` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:datalox, "~> 0.1.0"}
  ]
end
```

## Quick Start

```elixir
# Create a database
{:ok, db} = Datalox.new(name: :my_db)

# Assert facts
Datalox.assert(db, {:user, ["alice", :admin]})
Datalox.assert(db, {:user, ["bob", :viewer]})
Datalox.assert(db, {:permission, [:admin, :read]})
Datalox.assert(db, {:permission, [:admin, :write]})

# Query with pattern matching (use :_ for wildcards)
Datalox.query(db, {:user, [:_, :admin]})
#=> [{:user, ["alice", :admin]}]

# Load rules and derive new facts
Datalox.load_file(db, "rules/access_control.dl")
Datalox.query(db, {:can_access, ["alice", :_]})

# Clean up
Datalox.stop(db)
```

## Three Ways to Define Rules

### 1. Programmatic API

Build rules directly with the `Rule` struct:

```elixir
alias Datalox.Rule

rule = Rule.new(
  {:ancestor, [:X, :Z]},
  [{:parent, [:X, :Y]}, {:ancestor, [:Y, :Z]}]
)
```

### 2. Macro DSL

Define facts and rules in a module:

```elixir
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

Datalox.load_module(db, MyRules)
```

### 3. External `.dl` Files

Write standard Datalog syntax in `.dl` files:

```prolog
% facts
user("alice", admin).
user("bob", viewer).
permission(admin, read).
permission(admin, write).

% rules
can_access(User, Resource) :- user(User, Role), permission(Role, Resource).
active(User) :- user(User), not banned(User).
```

Load with:

```elixir
Datalox.load_file(db, "rules/access_control.dl")
```

## `.dl` File Syntax

### Facts

```prolog
predicate(arg1, arg2, ...).
```

Arguments can be atoms (`admin`), strings (`"alice"`), integers (`42`), floats (`3.14`), or wildcards (`_`).

### Rules

```prolog
head(X, Y) :- body1(X, Z), body2(Z, Y).
```

Variables are uppercase identifiers (`X`, `Name`, `CpuTotal`). Wildcards (`_`) match anything without binding.

### Negation

```prolog
active(User) :- user(User), not banned(User).
```

### Aggregation

```prolog
dept_count(Dept, N) :- employee(_, Dept), N = count(Dept).
total_sales(Person, T) :- sale(Person, Amount), T = sum(Amount).
```

Supported aggregation functions: `count`, `sum`, `min`, `max`, `avg`, `collect`.

### Arithmetic Guards

Comparison operators (`>`, `<`, `>=`, `<=`, `!=`) and arithmetic expressions (`+`, `-`, `*`, `/`) can be used in rule bodies. Operator precedence follows standard math: `*`/`/` bind tighter than `+`/`-`.

**Comparison guards** filter results:

```prolog
high_score(Name, S) :- score(Name, S), S > 50.
mid_score(Name, S) :- score(Name, S), S > 30, S < 80.
diff_pair(X, Y) :- pair(X, Y), X != Y.
```

**Arithmetic assignment** binds a new variable to a computed value:

```prolog
with_tax(Item, Total) :- price(Item, P), Total = P * 1.1.
free(Node, F) :- total(Node, T), used(Node, U), F = T - U.
```

**Arithmetic inside comparisons**:

```prolog
fast(X) :- time(X, T), start(X, S), T - S < 100.
```

**Parenthesized expressions** override precedence:

```prolog
r(X) :- v(X, A, B, C), (A + B) * C > 10.
```

**Combined example** with multiple body goals, assignment, and comparison:

```prolog
node_resources_free(Node, CpuFree, MemFree) :-
    node_resources(Node, CpuTotal, MemTotal),
    node_resources_used(Node, CpuUsed, MemUsed),
    CpuFree = CpuTotal - CpuUsed,
    MemFree = MemTotal - MemUsed.

with_tax(Item, Total) :- price(Item, P), Total = P * 1.1, Total > 5.
```

### Comments

```prolog
% This is a comment
user("alice", admin).  % Inline comments too
```

## Querying

```elixir
# Pattern matching with wildcards
Datalox.query(db, {:user, [:_, :admin]})

# Get first match
Datalox.query_one(db, {:user, ["alice", :_]})

# Check existence
Datalox.exists?(db, {:user, ["alice", :_]})

# Explain how a fact was derived
Datalox.explain(db, {:ancestor, ["alice", "carol"]})
```

## Features

- **Semi-naive evaluation** -- incremental fixpoint computation avoids redundant work
- **Stratified negation** -- safe handling of `not` in rule bodies
- **Aggregation** -- `count`, `sum`, `min`, `max`, `avg`, `collect` with grouping
- **Arithmetic guards** -- comparisons and arithmetic expressions parsed from `.dl` files
- **Magic Sets optimization** -- goal-directed evaluation for recursive queries with bound arguments
- **Pluggable storage** -- ETS backend by default, implements `Datalox.Storage` behaviour
- **Safety checking** -- validates all rules are safe before evaluation
- **Explain queries** -- inspect derivation trees to understand how facts were derived
- **Telemetry integration** -- query metrics and performance tracking

## License

MIT

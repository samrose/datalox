# Formal Verification Plan for Datalox

## 1. Architecture and Module Inventory

### 1.1 Complete Module Dependency Graph

```
Datalox (facade)
├── Datalox.Database (GenServer — central coordinator)
│   ├── Datalox.Evaluator (semi-naive fixed-point)
│   │   ├── Datalox.Unification (variable binding, pattern matching)
│   │   ├── Datalox.Aggregation (count/sum/min/max/avg/collect)
│   │   ├── Datalox.Join.Leapfrog (worst-case optimal join)
│   │   ├── Datalox.Optimizer.JoinOrder (cost-based reordering)
│   │   └── Datalox.Optimizer.Stratifier (negation stratification)
│   ├── Datalox.Incremental (delta-based maintenance)
│   │   └── Datalox.Unification
│   ├── Datalox.Optimizer.MagicSets (goal-directed rewriting)
│   ├── Datalox.Safety (range-restriction checking)
│   │   └── Datalox.Unification
│   └── Datalox.Storage (behaviour)
│       ├── Datalox.Storage.ETS (primary backend)
│       └── Datalox.Storage.CubDB (persistent backend)
├── Datalox.Parser.Parser (token stream → AST)
│   ├── Datalox.Parser.Lexer (NimbleParsec tokenizer)
│   └── Datalox.Rule (rule struct)
├── Datalox.DSL (compile-time macros)
├── Datalox.Explain (derivation trees)
├── Datalox.Functions (Agent — user-defined function registry)
├── Datalox.Metrics (GenServer — telemetry counters via ETS)
├── Datalox.Subscription (GenServer — change notifications)
├── Datalox.Lattice (behaviour + Min/Max/Set impls)
├── Datalox.Compiler.MatchSpec (ETS match spec compilation)
├── Datalox.Errors (exception structs)
└── Datalox.Application (supervisor tree)
```

### 1.2 Module Classification: Pure vs. Stateful

#### Pure Functional (no side effects, no ETS, no processes)

| Module | Role | Lines | Key Functions |
|--------|------|-------|---------------|
| `Datalox.Unification` | Variable detection, substitution, pattern unification | 80 | `variable?/1`, `substitute/2`, `unify/3`, `unify_one/3` |
| `Datalox.Aggregation` | Aggregate computation (count/sum/min/max/avg/collect) | 29 | `compute/3` |
| `Datalox.Rule` | Rule struct + dependency/variable analysis | 103 | `new/3`, `variables/1`, `head_variables/1`, `depends_on/1`, `recursive?/1` |
| `Datalox.Safety` | Range-restriction safety checking | 128 | `check_all/1` |
| `Datalox.Optimizer.Stratifier` | Negation stratification (dependency graph, stratum assignment) | 154 | `stratify/1`, `dependency_graph/1` |
| `Datalox.Optimizer.MagicSets` | Magic Sets query rewriting | 111 | `transform/2`, `magic_predicate/1` |
| `Datalox.Lattice` | Lattice behaviour + Min/Max/Set implementations | 49 | `bottom/0`, `join/2`, `leq?/2` |
| `Datalox.Explain` | Derivation tree struct | 31 | `base/1`, `derived/3` |
| `Datalox.Errors` | Exception struct definitions | 103 | (defexception only) |
| `Datalox.Compiler.MatchSpec` | Pattern → ETS match spec compilation | 32 | `compile/2` |
| `Datalox.Parser.Lexer` | NimbleParsec tokenizer | 130 | `tokenize/1` |
| `Datalox.Parser.Parser` | Token stream → facts/rules | 389 | `parse/2`, `parse_file/2` |

#### Stateful Shell (GenServer / Agent / ETS)

| Module | Role | State Type | Key Side Effects |
|--------|------|-----------|-----------------|
| `Datalox.Database` | Central coordinator GenServer | GenServer state with `storage_state`, `rules` | ETS via storage module, `Task.async_stream` in evaluator |
| `Datalox.Storage.ETS` | ETS storage backend | Map of ETS table refs | `:ets.new`, `:ets.insert`, `:ets.lookup`, `:ets.delete` |
| `Datalox.Storage.CubDB` | CubDB persistent backend | CubDB process ref | `CubDB.put`, `CubDB.select`, `CubDB.delete` |
| `Datalox.Functions` | User function registry | Agent state (map) | `Agent.get`, `Agent.update` |
| `Datalox.Metrics` | Telemetry counters | GenServer + named ETS table | `:ets.update_counter`, `:telemetry.execute` |
| `Datalox.Subscription` | Change notification pub/sub | GenServer state (list) | Callback invocation |
| `Datalox.Application` | OTP supervisor tree | Supervisor | Process lifecycle |

#### Mixed (business logic interleaved with side effects)

| Module | Pure Parts | Stateful Parts |
|--------|-----------|---------------|
| `Datalox.Evaluator` | `process_new_facts`, `merge_lattice_fact`, `apply_guards`, `evaluate_guard`, `eval_expr`, `apply_comparison`, `instantiate_head`, `substitute_value`, `matches_pattern?`, `group_independent_rules`, `project_bindings`, `apply_aggregations` | `evaluate/4` calls `storage_mod.lookup`, `storage_mod.insert`; `evaluate_group` uses `Task.async_stream`; all `evaluate_goal` functions call `storage_mod.lookup` |
| `Datalox.Incremental` | `affected_rules/2`, `compute_delta/3` (operates on in-memory facts map), `compute_retractions/3` | None directly — but called from `Database` GenServer which feeds it ETS-derived data |
| `Datalox.Join.Leapfrog` | `eligible?/1`, `intersect/1`, `do_leapfrog`, `seek` | `evaluate_join/3` calls `storage_mod.all` and `storage_mod.lookup` |
| `Datalox` (facade) | `query_one/2`, `assert_all/2`, `explain_fact`, `explain_body_goals` | All facade functions delegate to `Database` GenServer |
| `Datalox.Optimizer.JoinOrder` | Sorting logic | `reorder/4` calls `storage_mod.count` |
| `Datalox.DSL` | All macro logic | Compile-time only (module attributes) |

### 1.3 Pipeline Architecture

The Datalog pipeline flows as:

```
.dl source text
    │
    ▼
[Parser.Lexer] ─ tokenize ─► token list
    │
    ▼
[Parser.Parser] ─ parse ─► [Rule.t()] + [{atom, list}] facts
    │
    ▼
[Safety.check_all] ─ range restriction ─► validated rules
    │
    ▼
[Stratifier.stratify] ─ negation layers ─► strata: [[Rule.t()]]
    │
    ▼
[Evaluator.evaluate] ─ semi-naive fixpoint per stratum ─►
    │  ├── [JoinOrder.reorder] ─ cost-based goal ordering
    │  ├── [Leapfrog.evaluate_join] ─ WCOJ (if eligible)
    │  ├── [Unification.unify] ─ binding extension
    │  ├── [Aggregation.compute] ─ aggregate values
    │  └── [Lattice.join] ─ subsumptive merging
    │
    ▼
Derived facts stored in ETS via Storage.ETS
    │
    ▼
[Database.query] ─ pattern lookup (optionally via MagicSets)
```

### 1.4 Key Observations About the Evaluation Core

**Where the core evaluation loop lives:** `Datalox.Evaluator.evaluate/4` → `evaluate_strata` → `evaluate_stratum` → `iterate_seminaive`. The fixed-point loop is `iterate_seminaive/6`, which terminates when the delta (newly derived facts) is empty.

**Where rule matching and unification happen:** `Datalox.Unification.unify/3` is called from:
- `Evaluator.evaluate_goal/4` (main body evaluation)
- `Evaluator.evaluate_goal_against_facts/3` (delta evaluation)
- `Incremental.create_binding/2` and `Incremental.evaluate_goals/3`
- `Leapfrog.extend_binding/5`
- `Datalox.explain_body_goals/4`

**Where .dl parsing happens:** `Parser.Lexer.tokenize/1` (NimbleParsec) → `Parser.Parser.parse/2` (hand-written recursive descent on token stream).

**How ETS is used:** ETS is the **primary fact store**, not a cache. All base and derived facts live in ETS tables. The `Storage.ETS` module creates one `:duplicate_bag` table per predicate, keyed by first column value. Optional secondary indexes are separate ETS tables. The Evaluator reads from and writes to ETS during fixed-point computation — ETS is in the hot loop.

**Concurrency model:** The `Database` GenServer serializes all client operations (assert, retract, query, load_rules). However, within `Evaluator.evaluate_group/4`, independent rule groups (rules with different head predicates) are evaluated in parallel via `Task.async_stream`. This means the storage state is read concurrently by multiple tasks during evaluation.

## 2. Purity Boundary Assessment

### 2.1 What Is Already Pure

The following modules are **completely pure** and can be formally verified directly:

1. **`Unification`** — The most critical pure module. All functions are pure `(terms, values, binding) → {:ok, binding} | :fail`.

2. **`Aggregation`** — Pure `(fn, values, opts) → result`. Trivially verifiable.

3. **`Rule`** — Pure struct operations. `variables/1`, `depends_on/1`, `recursive?/1` are all pure MapSet computations.

4. **`Safety`** — Pure analysis `[Rule.t()] → :ok | {:error, [String.t()]}`. Depends only on `Unification.variable?/1`.

5. **`Stratifier`** — Pure graph algorithm `[Rule.t()] → {:ok, [[Rule.t()]]} | {:error, ...}`. Implements a fixed-point iteration over a dependency graph.

6. **`MagicSets`** — Pure rule transformation `([Rule.t()], query) → {seeds, [Rule.t()]}`.

7. **`Lattice.*`** — Pure algebraic structures with `bottom/0`, `join/2`, `leq?/2`.

8. **`Parser.Lexer`** — Pure tokenization (NimbleParsec-generated). `String.t() → {:ok, tokens, ...} | {:error, ...}`.

9. **`Parser.Parser`** — Mostly pure. `parse/2` is pure; `parse_file/2` reads filesystem (side effect is trivially separable).

10. **`Explain`** — Pure struct construction.

11. **`MatchSpec`** — Pure compilation `pattern → match_spec`.

### 2.2 Mixed Modules: Where Purity Ends

The following modules interleave pure logic with storage I/O. **No refactoring of datalox is required** — the Lean 4 specification models the *semantics* independently, not the Elixir code. This analysis identifies which implementation logic corresponds to what part of the formal spec, and where conformance tests need to focus.

**`Datalox.Evaluator`** is the most important mixed module. Its core algorithm is a standard semi-naive fixed-point iteration:

```
Input: rules, initial_facts
Output: initial_facts ∪ all_derived_facts

1. Stratify rules into strata S₁, S₂, ..., Sₖ
2. For each stratum Sᵢ:
   a. Δ₀ = derive_all(Sᵢ, current_facts)  -- initial pass
   b. all_derived = Δ₀
   c. While Δₙ ≠ ∅:
      Δₙ₊₁ = derive_with_delta(Sᵢ, current_facts, Δₙ) \ all_derived
      all_derived = all_derived ∪ Δₙ₊₁
   d. current_facts = current_facts ∪ all_derived
3. Return current_facts
```

This algorithm is what the Lean 4 specification formalizes. The Elixir implementation adds:
- Storage I/O (ETS reads/writes via `storage_mod`)
- `Task.async_stream` parallelism (semantically equivalent to sequential `Enum.flat_map`)
- Telemetry/metrics calls

These implementation concerns are **outside the scope of the formal spec**. Conformance testing validates that the real implementation's behavior matches the spec despite them.

The pure functions within the Evaluator (guard evaluation, head instantiation, pattern matching, aggregation dispatch, lattice merging) correspond directly to spec definitions and are covered by the conformance tests.

**`Datalox.Incremental`** is already nearly pure. `compute_delta/3` and `compute_retractions/3` operate on `existing_facts :: %{atom() => [[any()]]}` — a plain map. No changes needed.

**`Datalox.Join.Leapfrog`** — `eligible?/1` and `intersect/1` are pure. `evaluate_join/3` reads from storage, but its correctness property (producing the same results as nested-loop join) is specified and tested independently of the storage layer.

**`Datalox.Optimizer.JoinOrder`** — calls `storage_mod.count` for cost estimation. This is an optimization, not a correctness concern; any reordering of body goals produces the same results.

### 2.3 Can the Semantics Be Expressed as `evaluate(rules, facts) → facts`?

**Yes — and this is exactly what the Lean 4 specification does.** The specification models evaluation as a pure function over sets:

```
semiNaiveEval : List Rule → FactSet → FactSet
```

The Elixir implementation wraps this in ETS I/O and GenServer coordination. The formal spec does not need to model these — it captures the *mathematical semantics* that the implementation should conform to. Conformance testing bridges the gap.

### 2.4 Architecture Diagram: Pure vs. Stateful Boundaries

This diagram shows datalox **as it exists today** — no refactoring required. The Lean 4 specification models the semantics of the pure logic; conformance tests validate the full system.

```
┌──────────────────────────────────────────────────────────────┐
│                    IMPLEMENTATION (Elixir)                    │
│                                                              │
│  ┌─ Stateful coordination (outside formal spec scope) ────┐ │
│  │  Database (GenServer) ──► Storage.ETS (ETS tables)      │ │
│  │  Metrics (GenServer+ETS)   Functions (Agent)            │ │
│  │  Subscription (GenServer)  Application (Supervisor)     │ │
│  └─────────────────────────────────────────────────────────┘ │
│         │ calls                                              │
│  ┌─ Mixed modules (logic + storage I/O) ──────────────────┐ │
│  │  Evaluator    — semi-naive loop + ETS reads/writes      │ │
│  │  Leapfrog     — WCOJ algorithm + storage reads          │ │
│  │  JoinOrder    — cost-based reorder + storage count      │ │
│  └─────────────────────────────────────────────────────────┘ │
│         │ uses                                               │
│  ┌─ Pure modules (modeled directly by Lean 4 spec) ──────┐ │
│  │  Unification   Aggregation   Stratifier   Safety       │ │
│  │  MagicSets     Rule          Lattice      Explain      │ │
│  │  Incremental   MatchSpec     Parser       Errors       │ │
│  └────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
         ▲                              ▲
         │ conformance tests            │ formal proofs
         │ (StreamData)                 │ (Lean 4)
         │                              │
┌────────┴──────────────────────────────┴─────────────────────┐
│              LEAN 4 SPECIFICATION                            │
│  semiNaiveEval : List Rule → FactSet → FactSet              │
│  unify : List Term → List Term → Binding → Option Binding   │
│  stratify : List Rule → List (List Rule)                     │
│  T_P : List Rule → FactSet → FactSet (monotone)             │
└──────────────────────────────────────────────────────────────┘
```

## 3. Datalog Fragment Supported by Datalox

Before choosing a verification strategy, we must precisely characterize what fragment of Datalog datalox implements:

| Feature | Supported? | Evidence |
|---------|-----------|---------|
| Positive Datalog (recursive rules, no negation) | Yes | Core evaluation loop handles recursion via semi-naive fixed-point |
| Stratified negation | Yes | `Stratifier.stratify/1` computes strata; `filter_negations` in Evaluator |
| Aggregation (count, sum, min, max, avg, collect) | Yes | `Aggregation.compute/3`; parsed from `.dl` syntax `N = count(...)` |
| Arithmetic guards | Yes | `eval_expr` handles `+`, `-`, `*`, `/`; `evaluate_guard` handles `>`, `<`, `>=`, `<=`, `!=`, `=` |
| User-defined functions | Yes | `Functions` Agent registry; called via guards in evaluation |
| Lattice-based subsumptive evaluation | Yes | `Lattice` behaviour; `process_new_facts` merges via `lattice_mod.join` |
| Magic Sets optimization | Yes | `MagicSets.transform/2` for goal-directed evaluation of recursive queries |
| Incremental maintenance | Yes | `Incremental.compute_delta/3` and `compute_retractions/3` |
| Worst-case optimal joins (Leapfrog) | Yes | `Leapfrog.evaluate_join/3` with `gb_trees` |
| Well-founded negation | No | Only stratified negation |
| Constraint Datalog | No | No constraint propagation |
| Disjunction in heads | No | Single head predicate per rule |
| Non-ground facts | No | Facts must be fully ground |

This is a rich fragment — substantially more than "textbook Datalog" (which is just positive + stratified negation). The aggregation, lattice, and incremental features push toward Datalog with extensions (similar to Soufflé, Flix, or Differential Datalog).

## 4. Verification Strategy Evaluation

### Approach A: Lean 4 Verified Specification + Conformance Testing

**Idea:** Write a formal Lean 4 specification of Datalog semantics. Prove properties about the specification. Test the Elixir implementation against the specification.

**Pros:**
- Clean separation: the spec is the "truth", the implementation is tested against it
- Proves properties about the *semantics* of the Datalog fragment, not just the implementation
- No need to model Elixir/BEAM semantics in Lean 4
- Conformance tests bridge the gap between spec and implementation
- Most publishable: this is a formal semantics contribution

**Cons:**
- Does not prove the Elixir code is correct — only that the specification is correct and the implementation agrees on test cases
- Conformance testing coverage is inherently finite
- Requires writing a Lean 4 reference evaluator (non-trivial)

**Feasibility:** HIGH. This is the most tractable approach and produces the most publishable results.

### Approach B: Faithful Translation to Lean 4 + Direct Proofs

**Idea:** Translate the pure Elixir modules into structurally equivalent Lean 4 code. Prove properties on the translation.

**Pros:**
- Proves properties about code that structurally mirrors the real implementation
- Can verify specific algorithmic choices (e.g., Leapfrog's correctness)

**Cons:**
- Elixir → Lean 4 translation is non-trivial:
  - Elixir atoms (`String.to_atom`) have no Lean 4 equivalent; must model as strings or an inductive type
  - Pattern matching on dynamic types (`any()`) is pervasive; Lean 4 is strongly typed
  - `Map.get/3` with default values, `Enum.*` pipelines, list comprehensions — all need manual translation
  - No existing Elixir-to-Lean transpiler
- Translation fidelity is itself an unverified assumption
- Significant manual effort for ~1,500 lines of pure code

**Feasibility:** MEDIUM. Useful for specific modules (Unification, Stratifier) but impractical for the full system. **This is the only approach that would benefit from refactoring datalox** (extracting a pure Evaluator kernel to translate). We do not recommend this path.

### Approach C: TLA+ Model Checking for Stateful Layer

**Idea:** Model the Database GenServer state machine and ETS interactions in TLA+. Check liveness/safety properties.

**Pros:**
- Directly addresses the GenServer/ETS coordination, which formal proofs cannot
- TLA+ excels at modeling concurrent state machines
- Can verify: "evaluation always reaches fixed point", "no partial state visible to queries during evaluation", "incremental maintenance preserves equivalence"
- Well-suited for the `Task.async_stream` parallelism in the Evaluator

**Cons:**
- TLA+ is model checking (finite state exploration), not theorem proving
- Cannot prove general termination or correctness — only for finite models
- The interesting concurrency in Datalox is limited (GenServer serializes most operations)
- Less publishable on its own; TLA+ contributions need novel modeling insights

**Feasibility for peer-reviewed publication:** MEDIUM-LOW standalone. The GenServer serialization means the concurrency story is thin for an academic audience.

**Feasibility for confidence-building and community review:** HIGH. The real value is not novelty but *concrete answers* to questions like "can parallel evaluation groups sharing ETS produce inconsistent derivations?" A self-published TLA+ model that people can read, run, and critique is a strong trust signal.

### Approach D: Hybrid (RECOMMENDED)

**Idea:** Combine the strongest elements:
1. **Lean 4 formal specification** of the Datalog semantics datalox implements (Approach A core)
2. **Lean 4 proofs** of key properties on the specification
3. **Lean 4 verified reference evaluator** that can generate test oracles
4. **Property-based conformance testing** (StreamData) bridging spec → implementation
5. **TLA+ model** for the GenServer/ETS coordination layer — targeting community trust, not publication novelty

**Crucially, this approach requires no changes to datalox itself.** The Lean 4 specification is a standalone artifact that models the mathematical semantics of the Datalog fragment datalox implements. The TLA+ model is a standalone artifact that models the stateful coordination layer. Conformance tests run the real Elixir implementation against the Lean 4 spec to validate agreement.

**Why this is best:**
- Datalox stays as-is — the formal work is additive, not invasive
- The specification captures the semantics independent of implementation choices (ETS, GenServer, etc.)
- The TLA+ model covers what the Lean 4 proofs cannot: the actual concurrency and state management
- Together, Lean 4 + TLA+ + conformance testing provide coverage across the entire system — pure semantics, stateful coordination, and implementation fidelity
- Conformance testing via StreamData (already a dependency) provides the bridge
- The combination is a strong foundation for self-publication and community review

## 5. Recommended Approach: Detailed Plan

### Phase 1: Lean 4 Formal Specification (Weeks 1-4)

> **Note:** This phase begins immediately — no refactoring of datalox is required. The specification models the Datalog semantics independently of the Elixir implementation.

#### 5.1 Core Types

```lean
-- Datalog terms and facts
inductive Term where
  | var : String → Term
  | const : String → Term
  | int : Int → Term
  | str : String → Term

def Predicate := String
def Tuple := List Term
def Fact := Predicate × List Term  -- ground tuple (no variables)
def FactSet := Finset Fact

-- Rules
structure Rule where
  head : Predicate × List Term
  body : List (Predicate × List Term)
  negations : List (Predicate × List Term)

-- Binding (substitution)
def Binding := String → Option Term

-- Database (logical model)
structure Database where
  facts : FactSet
  rules : List Rule
```

#### 5.2 Semantic Definitions

```lean
-- Immediate consequence operator (T_P)
def immediateConsequence (rules : List Rule) (I : FactSet) : FactSet :=
  rules.foldl (fun acc rule =>
    acc ∪ deriveFromRule rule I
  ) ∅

-- Semi-naive evaluation
def semiNaiveEval (rules : List Rule) (base : FactSet) : FactSet :=
  let rec loop (current : FactSet) (delta : FactSet) (fuel : Nat) :=
    match fuel with
    | 0 => current  -- termination bound
    | n + 1 =>
      let newDelta := immediateConsequence rules (current ∪ delta) \ current
      if newDelta = ∅ then current ∪ delta
      else loop (current ∪ delta) newDelta n
  loop base (immediateConsequence rules base \ base) (rules.length * base.card + 1)

-- Stratified evaluation
def stratifiedEval (strata : List (List Rule)) (base : FactSet) : FactSet :=
  strata.foldl (fun facts stratum =>
    semiNaiveEval stratum facts
  ) base
```

#### 5.3 Unification Specification

```lean
-- Unification of a single term against a ground value
def unifyOne (term : Term) (value : Term) (σ : Binding) : Option Binding :=
  match term with
  | Term.var x =>
    match σ x with
    | some v => if v = value then some σ else none
    | none => some (fun y => if y = x then some value else σ y)
  | _ => if term = value then some σ else none

-- Unification of term lists
def unify : List Term → List Term → Binding → Option Binding
  | [], [], σ => some σ
  | t :: ts, v :: vs, σ =>
    match unifyOne t v σ with
    | some σ' => unify ts vs σ'
    | none => none
  | _, _, _ => none
```

### Phase 2: Key Property Proofs (Weeks 4-8)

### 5.4 Verification Target Inventory

Ranked by (value × feasibility):

| # | Property | Difficulty | Value | Module |
|---|----------|-----------|-------|--------|
| 1 | **Unification correctness**: `unify` produces a most-general unifier | Low | High | Unification |
| 2 | **Unification idempotence**: `unify(ts, vs, σ) = some σ' → unify(ts, vs, σ') = some σ'` | Low | Medium | Unification |
| 3 | **Fixed-point existence**: semi-naive evaluation reaches a fixed point of T_P | Medium | Critical | Evaluator |
| 4 | **Soundness**: every derived fact is in the minimal model T_P↑ω(∅) | Medium | Critical | Evaluator |
| 5 | **Completeness**: every fact in the minimal model is derived | Medium | Critical | Evaluator |
| 6 | **Semi-naive ≡ naive**: semi-naive evaluation produces the same result as naive bottom-up | Medium | High | Evaluator |
| 7 | **Stratification well-definedness**: stratify terminates and produces a valid stratification when no negative cycle exists | Medium | High | Stratifier |
| 8 | **Stratification completeness**: if a valid stratification exists, `stratify` finds one | Medium | Medium | Stratifier |
| 9 | **Safety soundness**: if `check_all` passes, every head variable is range-restricted | Low | Medium | Safety |
| 10 | **Aggregation monotonicity**: aggregation over lattices preserves monotonicity | Medium | Medium | Aggregation + Lattice |
| 11 | **Leapfrog correctness**: `intersect` returns exactly the set intersection | Low | Medium | Leapfrog |
| 12 | **Magic Sets equivalence**: magic-transformed rules produce the same relevant facts as the original rules for the query | High | High | MagicSets |
| 13 | **Incremental equivalence**: incremental maintenance produces the same result as full re-evaluation | High | High | Incremental |
| 14 | **Termination**: semi-naive evaluation terminates for any finite Datalog program over a finite domain | Medium | Critical | Evaluator |
| 15 | **Parser round-trip**: `parse(render(rule)) = rule` for well-formed rules | Low | Low | Parser |

### 5.5 Concrete Proof Sketches for Top 3 Properties

#### Proof 1: Unification Correctness (Properties #1, #2)

```lean
/-- A binding σ is a unifier for terms ts and values vs if
    substituting σ into ts yields vs -/
def IsUnifier (σ : Binding) (ts vs : List Term) : Prop :=
  ts.map (substitute σ) = vs

/-- Unification is sound: if unify succeeds, the result is a unifier -/
theorem unify_sound :
  ∀ (ts vs : List Term) (σ σ' : Binding),
    unify ts vs σ = some σ' →
    IsUnifier σ' ts vs := by
  intro ts vs σ σ' h
  induction ts, vs using List.rec₂ with
  | nil_nil => simp [unify] at h; subst h; simp [IsUnifier, substitute]
  | cons_cons t ts v vs ih =>
    simp [unify] at h
    obtain ⟨σ₁, h₁, h₂⟩ := h
    -- unifyOne t v σ = some σ₁ means σ₁ unifies t with v
    have := unifyOne_sound h₁
    -- By IH, σ' unifies ts with vs under σ₁
    exact ⟨this, ih h₂⟩

/-- Unification is most-general: any other unifier is an instance -/
theorem unify_mgu :
  ∀ (ts vs : List Term) (σ σ' τ : Binding),
    unify ts vs σ = some σ' →
    IsUnifier τ ts vs →
    ∃ (δ : Binding), τ = compose σ' δ := by
  -- By induction on ts, using the fact that unifyOne extends minimally
  sorry  -- Requires detailed induction; standard textbook proof
```

**Effort estimate:** 2-3 days for `unify_sound`, 3-5 days for `unify_mgu`.

#### Proof 2: Fixed-Point Correctness (Properties #3, #4, #5)

```lean
/-- The immediate consequence operator T_P -/
def T_P (rules : List Rule) (I : FactSet) : FactSet :=
  ⋃ r ∈ rules, { f | ∃ σ, bodyMatch r.body σ I ∧ f = instantiate r.head σ }

/-- T_P is monotone: I ⊆ J → T_P(I) ⊆ T_P(J) -/
theorem T_P_monotone (rules : List Rule) :
  ∀ I J : FactSet, I ⊆ J → T_P rules I ⊆ T_P rules J := by
  intro I J h_sub f h_f
  obtain ⟨r, hr, σ, h_match, h_inst⟩ := h_f
  exact ⟨r, hr, σ, bodyMatch_monotone h_sub h_match, h_inst⟩

/-- For positive Datalog over finite domains, T_P has a least fixed point -/
theorem lfp_exists (rules : List Rule) (base : FactSet) :
  ∃ n : Nat, T_P rules (iterate_T_P rules base n) = iterate_T_P rules base n := by
  -- By finiteness of the Herbrand base and monotonicity of T_P,
  -- the ascending chain base ⊆ T_P(base) ⊆ T_P²(base) ⊆ ...
  -- must stabilize within |Herbrand base| steps.
  -- This follows from Knaster-Tarski for finite lattices.
  sorry

/-- Semi-naive evaluation computes the same result as naive iteration -/
theorem seminaive_eq_naive (rules : List Rule) (base : FactSet) :
  semiNaiveEval rules base = naiveEval rules base := by
  -- Key insight: in each iteration, semi-naive derives exactly the facts
  -- that naive would derive minus those already known.
  -- The delta Δₙ = T_P(all_n) \ all_{n-1} captures precisely the new facts.
  -- Since T_P(all_n) = T_P(all_{n-1} ∪ Δ_{n-1}) and T_P is monotone,
  -- and semi-naive only uses Δ_{n-1} for the "new" part of the join,
  -- the result is identical.
  sorry
```

**Effort estimate:** 2-3 weeks. The monotonicity proof is straightforward. The Knaster-Tarski argument requires formalizing the Herbrand base finiteness. The semi-naive ≡ naive equivalence is the hardest — it requires a careful induction showing the delta tracking is correct.

#### Proof 3: Stratification Correctness (Properties #7, #8)

```lean
/-- A stratification is valid if negated predicates are in strictly earlier strata -/
def ValidStratification (strata : List (List Rule)) : Prop :=
  ∀ i, ∀ r ∈ strata[i],
    -- Positive dependencies can be in same or earlier stratum
    (∀ p ∈ positiveDeps r, stratumOf strata p ≤ i) ∧
    -- Negative dependencies must be in strictly earlier stratum
    (∀ p ∈ negativeDeps r, stratumOf strata p < i)

/-- The stratifier produces a valid stratification when it succeeds -/
theorem stratify_valid :
  ∀ (rules : List Rule) (strata : List (List Rule)),
    stratify rules = ok strata →
    ValidStratification strata := by
  intro rules strata h
  -- The fixed-point iteration assigns stratum(p) = max(
  --   max{stratum(q) | q ∈ positiveDeps(p)},
  --   max{stratum(q) + 1 | q ∈ negativeDeps(p)}
  -- )
  -- When this converges, the assignment satisfies the stratification conditions
  -- by construction.
  sorry

/-- If no negative cycle exists, stratification succeeds -/
theorem stratify_complete :
  ∀ (rules : List Rule),
    ¬HasNegativeCycle (dependencyGraph rules) →
    ∃ strata, stratify rules = ok strata := by
  -- The fixed-point must converge in at most |predicates| iterations
  -- because without negative cycles, stratum assignments are bounded
  -- by the longest path in the dependency graph.
  sorry
```

**Effort estimate:** 1-2 weeks. The stratification algorithm is a classic fixed-point computation and the proofs follow standard graph-theoretic arguments.

### Phase 3: Conformance Testing (Weeks 6-10)

Build a StreamData-powered conformance test suite:

1. **Random Datalog program generator**: Generate random positive Datalog programs (random predicates, arities, rules, facts) using StreamData.
2. **Reference evaluator**: Run the Lean 4 specification (extracted to executable code) on the same input.
3. **Comparison**: Assert that `datalox_result == lean_reference_result` for each generated program.

Key test generators:
- Random ground facts
- Random positive rules (no negation initially)
- Random rules with stratified negation
- Random queries with bound/unbound patterns
- Random incremental updates (assert/retract/query sequences)

### Phase 4: TLA+ Model for Stateful Layer (Weeks 8-12)

The Lean 4 specification covers the pure Datalog semantics. The TLA+ model covers what Lean 4 cannot: the GenServer state machine, ETS interactions, and the `Task.async_stream` parallelism. The goal is not academic novelty but concrete answers to specific safety questions — artifacts that people can read, run with TLC, and critique.

#### What to model

**1. Database GenServer state machine**
```
States: init | idle | asserting | retracting | querying | loading_rules | evaluating
Transitions: init → idle → (assert | retract | query | load_rules)*
```
The GenServer serializes all client operations. This is straightforward to model and confirms the basic safety guarantee: no interleaved client operations.

**2. `Task.async_stream` parallel evaluation (PRIMARY TARGET)**

This is the most valuable part. In `Evaluator.evaluate_group/4`, independent rule groups are evaluated in parallel:

```elixir
group
|> Task.async_stream(fn rule -> evaluate_rule(rule, st, storage_mod, delta) end, ordered: false)
```

All parallel tasks share the same `st` (storage state containing ETS table refs). They read from the same ETS tables concurrently. After all tasks complete, `store_facts/3` writes all results sequentially.

The TLA+ model should capture:
- Multiple evaluation tasks reading ETS concurrently
- Each task producing a set of derived facts
- `store_facts` writing facts to ETS after all tasks complete
- The deduplication in `process_new_facts` that compensates for potential duplicates

**Key property to check:** `ParallelEvalEquivalence` — the set of derived facts is identical regardless of task scheduling order. This requires showing that:
- Read-only access during parallel evaluation means tasks don't interfere
- The `store_facts` sequential write phase happens only after all reads complete
- Deduplication in `process_new_facts` makes the final result set-equivalent

**3. Incremental maintenance atomicity**

When `handle_call({:assert, pred, tuple})` triggers `incrementally_derive`, derived facts are written to ETS one-by-one via `Enum.reduce`. Model this to confirm:
- No query can observe partial incremental state (guaranteed by GenServer serialization)
- The final state after incremental maintenance matches what full re-evaluation would produce (for the finite model)

**4. Magic Sets temporary storage lifecycle**

`evaluate_with_magic_sets` creates temporary ETS tables, copies base facts, evaluates, and cleans up. Model the lifecycle to check:
- Temp tables are always cleaned up (even if evaluation fails)
- No reference to temp tables escapes the `handle_call` scope

#### TLA+ invariants to check

```tla
\* No client operation interleaves with another
ClientSerialization == \A i, j \in ClientOps : i /= j => ~(InProgress(i) /\ InProgress(j))

\* Parallel evaluation produces same result as sequential
ParallelEvalEquivalence ==
  LET parallel_result == ParallelEval(groups, storage)
      sequential_result == SequentialEval(groups, storage)
  IN SetEqual(parallel_result, sequential_result)

\* No temp storage leaks
TempStorageCleanup == <>[](\A t \in TempTables : ~Exists(t))

\* Incremental result matches full re-evaluation
IncrementalCorrectness ==
  LET incremental_state == IncrementalDerive(state, new_fact)
      full_state == FullReEvaluate(state.rules, state.base_facts \union {new_fact})
  IN SetEqual(DerivedFacts(incremental_state), DerivedFacts(full_state))
```

#### Deliverable

A self-contained TLA+ specification (`tla/DataloxCoordination.tla`) with:
- The model
- All invariants and temporal properties
- TLC configuration for model checking with small instances (2-3 predicates, 2-3 rules, 3-5 facts)
- A README explaining what was checked, what the results mean, and how to re-run

## 6. Publication Strategy: Self-Publishing for Community Review

The goal is not a peer-reviewed venue but a self-published artifact that invites community scrutiny and builds trust in datalox's correctness. All artifacts (Lean 4 proofs, TLA+ models, conformance tests) are open source alongside datalox.

### Writeup Outline

**Title:** "Verified Datalog on the BEAM: Formal Specification, Model Checking, and Conformance Testing for Datalox"

**Abstract (Draft):**
We present a formal verification effort for Datalox, a feature-rich Datalog implementation in Elixir running on the BEAM virtual machine. We develop a Lean 4 specification of the Datalog fragment implemented by Datalox — including semi-naive evaluation, stratified negation, aggregation, and lattice-based subsumptive evaluation — and prove key correctness properties: unification soundness, fixed-point existence, evaluation soundness and completeness, and stratification validity. We model the stateful coordination layer (GenServer, ETS, parallel evaluation) in TLA+ and check safety properties including parallel evaluation determinism and incremental maintenance correctness. We bridge the formal artifacts to the running Elixir code through property-based conformance testing using StreamData. All artifacts are open source and designed for community review.

**Structure:**

1. **Introduction** — What datalox is, why formal verification matters for a rule engine, what we verified and what we didn't
2. **Datalox Architecture** — System overview, pure/stateful boundary analysis, supported Datalog fragment
3. **Lean 4 Specification** — Core types, T_P operator, semi-naive evaluation, unification, stratification
4. **Formal Proofs** — Unification correctness, fixed-point existence, soundness/completeness, stratification validity
5. **TLA+ Model** — GenServer state machine, `Task.async_stream` parallelism safety, incremental maintenance, temp storage lifecycle. What TLC found (or confirmed safe).
6. **Conformance Testing** — StreamData generators, test oracle, what's tested vs. what's proven
7. **Verification Boundaries** — Honest accounting of what is proven, what is model-checked, what is tested, and what remains unverified
8. **How to Review** — Guide for community reviewers: how to run the Lean 4 proofs, how to run TLC on the TLA+ specs, how to run the conformance tests

### Publication Channels

| Channel | Purpose |
|---------|---------|
| **GitHub repository** (primary) | Lean 4 project, TLA+ specs, conformance tests — all runnable, all reviewable |
| **Blog post / writeup** | Human-readable narrative linking to the artifacts |
| **Elixir Forum / ElixirConf** | Community visibility in the BEAM ecosystem |
| **Erlang Workshop (ICFP)** | Optional future submission if the community feedback is strong and the results warrant it |

### What Makes This Credible for Community Review

1. **Runnable artifacts, not just claims**: Anyone can clone the repos, run `lake build` on the Lean 4 proofs, run TLC on the TLA+ specs, run `mix test` on the conformance tests.
2. **Honest boundaries**: The writeup explicitly states what is proven (Lean 4), what is model-checked for finite instances (TLA+), what is tested (conformance), and what is none of the above.
3. **The TLA+ model answers specific questions**: "Can parallel evaluation groups produce inconsistent results?" is not a vague claim — it's a TLC run with a concrete answer.
4. **Lean 4 proofs are machine-checked**: They either type-check or they don't. No ambiguity.

## 7. Effort Estimates

No changes to datalox are required. All work is additive (new Lean 4 project + TLA+ specs + new test files).

| Phase | Tasks | Estimated Effort | Dependencies |
|-------|-------|-----------------|-------------|
| **1: Core Spec** | Lean 4 types, T_P, unification, semi-naive eval | 2-3 weeks | None |
| **2: Proofs — Easy** | Unification soundness, stratification validity, safety soundness | 2-3 weeks | Phase 1 |
| **3: Proofs — Hard** | Fixed-point existence, soundness/completeness, semi-naive ≡ naive | 3-5 weeks | Phase 2 |
| **4: TLA+ Model** | GenServer state machine, `Task.async_stream` parallelism, incremental maintenance, temp storage lifecycle | 2-3 weeks | None (can run in parallel with Phases 1-3) |
| **5: Conformance Tests** | StreamData generators, test oracle, comparison harness | 1-2 weeks | Phase 1 |
| **6: Writing & Publication** | Self-published writeup, diagrams, community review preparation | 2-3 weeks | Phases 2-5 |
| **Total** | | **12-19 weeks** | |

Phases 1-3 (Lean 4) and Phase 4 (TLA+) are independent and can proceed in parallel if resources allow.

## 8. Risks and Limitations

### What Can't Be Proven

1. **ETS correctness**: We take ETS as a trusted primitive. ETS is implemented in C inside the BEAM VM — verifying it is outside scope. The Storage behaviour abstraction means our proofs hold for *any* correct storage implementation.

2. **GenServer ordering guarantees**: We assume the BEAM's message ordering guarantees (FIFO between two processes). This is a runtime guarantee, not something we verify.

3. **`Task.async_stream` determinism**: The parallel evaluation in `evaluate_group/4` uses `Task.async_stream` with `ordered: false`. The TLA+ model checks this for finite instances — confirming (or refuting) that parallel evaluation produces the same derived facts as sequential evaluation. However, TLA+ model checking covers only the finite state space explored by TLC, not a general proof. The key argument is that parallel tasks only read from ETS during evaluation (no writes until `store_facts`), and `process_new_facts` deduplicates the combined results. The TLA+ model makes this argument precise and machine-checkable for small instances.

4. **Parser correctness**: The NimbleParsec lexer is generated code — we'd need to verify the NimbleParsec library itself to fully verify the parser. We treat it as a trusted dependency and test the parser via conformance testing.

5. **Termination for all inputs**: Datalog termination over finite domains is a theorem, but our implementation's termination depends on the Herbrand base being finite. If users register external functions via `Datalox.Functions` that generate new values (atoms, strings), the domain could grow unboundedly, breaking termination. We prove termination *conditioned on* a finite domain.

6. **Incremental correctness under arbitrary update sequences**: `compute_retractions/3` uses a simple truth-maintenance approach that may over-retract in the presence of alternative derivations. A full proof of incremental equivalence with re-evaluation is the hardest verification target and may require significant additional formalization.

### Known Gaps in the Implementation

1. **Duplicate bag semantics in ETS**: `Storage.ETS` uses `:duplicate_bag` but manually deduplicates on insert. If deduplication has bugs, the semi-naive algorithm could loop or produce incorrect results. This is a testable but tricky correctness concern.

2. **Aggregation under stratified negation**: The interaction between aggregation and negation stratification is not clearly specified. Aggregation predicates should arguably be in their own stratum. The current Stratifier does not special-case aggregation.

3. **MagicSets transformation correctness**: The Magic Sets transformation in `MagicSets.transform/2` is a complex rewriting. Proving it preserves query equivalence is one of the hardest targets. The current implementation makes simplifying assumptions (e.g., `hd(goal_vars)` for propagation rules) that may not be correct for all rule shapes.

4. **Leapfrog + delta interaction**: The Leapfrog join (`evaluate_join/3`) is only used in the initial full-evaluation pass (when `delta` is empty). It's unclear if it interacts correctly with the semi-naive delta tracking. This is not a bug per se — Leapfrog is simply not used in delta mode — but it means a significant optimization is only active in the first iteration.

### Methodological Risks

1. **Lean 4 learning curve**: If the team has limited Lean 4 experience, the proof effort could be 2-3x the estimate. Mitigation: start with the easiest proofs (unification) and build up.

2. **Specification-implementation gap**: The biggest risk is that the Lean 4 specification doesn't accurately model what the Elixir code does. Since we do not modify datalox, the specification must be carefully designed to match the implementation's actual behavior. Conformance testing mitigates this, but can't eliminate it. Key divergence points: how `matches_pattern?` handles type mismatches, how `String.to_atom` works, how ETS deduplication interacts with the evaluator.

3. **Scope creep**: The temptation to verify everything (parser, incremental, magic sets) will delay the core contribution. Recommendation: prove properties #1-#7 from the inventory and leave the rest as future work.

4. **Reviewer expectations at different venues**: CPP reviewers expect deep mechanized proofs; Erlang Workshop reviewers expect practical relevance. The methodology and case study framing is more suited to the latter.

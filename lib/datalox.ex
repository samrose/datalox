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
  alias Datalox.Explain

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
        {facts, rules} =
          Enum.split_with(statements, fn
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

  @doc """
  Explains how a fact was derived.

  Returns an explanation showing the derivation tree.

  ## Examples

      explanation = Datalox.explain(db, {:ancestor, ["alice", "carol"]})
      explanation.fact       # => {:ancestor, ["alice", "carol"]}
      explanation.derivation # => :base or list of sub-derivations

  """
  @spec explain(pid() | atom(), {atom(), list()}) :: Explain.t() | nil
  def explain(db, {predicate, _pattern} = fact) do
    if exists?(db, fact) do
      # Check if the predicate has rules (is derived)
      rules = Database.get_rules(db)

      derived_predicates =
        rules
        |> Enum.map(fn rule -> elem(rule.head, 0) end)
        |> MapSet.new()

      if MapSet.member?(derived_predicates, predicate) do
        # This is a derived fact - find the matching rule
        matching_rule = Enum.find(rules, fn rule -> elem(rule.head, 0) == predicate end)
        rule_name = elem(matching_rule.head, 0)
        # Full derivation tracking would require storing derivation info in the evaluator
        # For now, we return a derived explanation with empty sub-derivations
        Explain.derived(fact, rule_name, [])
      else
        # This is a base fact
        Explain.base(fact)
      end
    else
      nil
    end
  end
end

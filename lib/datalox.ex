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
  alias Datalox.Unification

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
    Enum.reduce_while(facts, :ok, fn fact, :ok ->
      case assert(db, fact) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
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

    with :ok <- assert_all(db, facts) do
      Database.load_rules(db, rules)
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

        with :ok <- assert_all(db, fact_tuples) do
          Database.load_rules(db, rule_structs)
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
  def explain(db, {_predicate, _pattern} = fact) do
    if exists?(db, fact) do
      explain_fact(db, fact)
    else
      nil
    end
  end

  defp explain_fact(db, {predicate, args} = fact) do
    rules = Database.get_rules(db)

    matching_rules =
      Enum.filter(rules, fn rule -> elem(rule.head, 0) == predicate end)

    if matching_rules == [] do
      Explain.base(fact)
    else
      Enum.find_value(matching_rules, &try_explain_rule(db, rules, &1, args, fact, predicate)) ||
        Explain.base(fact)
    end
  end

  defp try_explain_rule(db, rules, rule, args, fact, predicate) do
    {_head_pred, head_vars} = rule.head

    case Unification.unify(head_vars, args, %{}) do
      {:ok, binding} ->
        case explain_body_goals(db, rules, rule.body, binding) do
          nil -> nil
          body_explanations -> Explain.derived(fact, predicate, body_explanations)
        end

      :fail ->
        nil
    end
  end

  defp explain_body_goals(_db, _rules, [], _binding), do: []

  defp explain_body_goals(db, rules, [goal | rest], binding) do
    {pred, goal_terms} = goal
    pattern = Enum.map(goal_terms, &Unification.substitute(&1, binding))

    # Query matching facts from the database
    matching = query(db, {pred, pattern})

    case matching do
      [] ->
        nil

      [{^pred, fact_args} | _] ->
        # Extend binding with the matched fact
        extended_binding =
          case Unification.unify(goal_terms, fact_args, binding) do
            {:ok, new_binding} -> new_binding
            :fail -> binding
          end

        sub_explanation = explain_fact(db, {pred, fact_args})

        case explain_body_goals(db, rules, rest, extended_binding) do
          nil -> nil
          rest_explanations -> [sub_explanation | rest_explanations]
        end
    end
  end
end

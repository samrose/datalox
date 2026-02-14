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

  require Logger

  alias Datalox.{Evaluator, Incremental, Rule, Storage}
  alias Datalox.Optimizer.MagicSets

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
  Loads rules into the database.
  """
  @spec load_rules(t() | atom(), [Rule.t()]) :: :ok | {:error, term()}
  def load_rules(db, rules) when is_list(rules) do
    GenServer.call(resolve(db), {:load_rules, rules})
  end

  @doc """
  Stops the database.
  """
  @spec stop(t() | atom()) :: :ok
  def stop(db) do
    GenServer.stop(resolve(db))
  end

  @doc """
  Returns the rules loaded in the database.
  """
  @spec get_rules(t() | atom()) :: [Rule.t()]
  def get_rules(db) do
    GenServer.call(resolve(db), :get_rules)
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
        Datalox.Metrics.record_assert(state.name, predicate)
        new_state = %{state | storage_state: new_storage_state}

        # Incremental maintenance: derive new facts if rules are loaded
        new_state =
          if state.rules != [] do
            incrementally_derive(new_state, predicate, tuple)
          else
            new_state
          end

        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:retract, predicate, tuple}, _from, state) do
    case state.storage_mod.delete(state.storage_state, predicate, tuple) do
      {:ok, new_storage_state} ->
        Datalox.Metrics.record_retract(state.name, predicate)
        new_state = %{state | storage_state: new_storage_state}

        # Incremental maintenance: retract derived facts if rules are loaded
        new_state =
          if state.rules != [] do
            incrementally_retract(new_state, predicate, tuple)
          else
            new_state
          end

        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:query, predicate, pattern}, _from, state) do
    # Record query metrics
    Datalox.Metrics.record_query(state.name, predicate)

    results =
      if should_use_magic_sets?(predicate, pattern, state.rules) do
        evaluate_with_magic_sets(predicate, pattern, state)
      else
        standard_lookup(state, predicate, pattern)
      end

    {:reply, results, state}
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
  def handle_call({:load_rules, rules}, _from, state) do
    case Datalox.Safety.check_all(rules) do
      {:error, errors} ->
        {:reply, {:error, {:safety_violations, errors}}, state}

      :ok ->
        case Evaluator.evaluate(rules, state.storage_state, state.storage_mod) do
          {:ok, _derived, new_storage_state} ->
            Datalox.Metrics.record_load_rules(state.name, length(rules))
            new_state = %{state | rules: rules, storage_state: new_storage_state}
            {:reply, :ok, new_state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  @impl true
  def handle_call(:get_rules, _from, state) do
    {:reply, state.rules, state}
  end

  @impl true
  def handle_call(:get_name, _from, state) do
    {:reply, {:ok, state.name}, state}
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

  defp standard_lookup(state, predicate, pattern) do
    case state.storage_mod.lookup(state.storage_state, predicate, pattern) do
      {:ok, results} ->
        Enum.map(results, fn tuple -> {predicate, tuple} end)

      {:error, reason} ->
        Logger.warning("Datalox: lookup failed for #{predicate}: #{inspect(reason)}")
        []
    end
  end

  # Check if magic sets optimization should be used:
  # 1. The predicate has recursive rules
  # 2. The pattern has at least one bound argument
  defp should_use_magic_sets?(_predicate, _pattern, []), do: false

  defp should_use_magic_sets?(predicate, pattern, rules) do
    has_bound_args = Enum.any?(pattern, &(&1 != :_))

    has_recursive_rules =
      Enum.any?(rules, fn rule ->
        {head_pred, _} = rule.head
        head_pred == predicate and Rule.recursive?(rule)
      end)

    has_bound_args and has_recursive_rules
  end

  defp evaluate_with_magic_sets(predicate, pattern, state) do
    {seeds, transformed_rules} = MagicSets.transform(state.rules, {predicate, pattern})

    # Create a temporary storage for magic sets evaluation
    temp_name = :"#{state.name}_magic_#{:erlang.unique_integer()}"
    {:ok, temp_storage} = state.storage_mod.init(name: temp_name)

    # Copy base facts from the original storage into temp
    temp_storage = copy_base_facts(state, temp_storage)

    # Insert magic seed facts
    temp_storage =
      Enum.reduce(seeds, temp_storage, fn {pred, tuple}, st ->
        {:ok, st} = state.storage_mod.insert(st, pred, tuple)
        st
      end)

    # Evaluate the transformed rules
    case Evaluator.evaluate(transformed_rules, temp_storage, state.storage_mod) do
      {:ok, derived, temp_storage} ->
        # Filter results to match the original query pattern
        results =
          Enum.filter(derived, fn {pred, tuple} ->
            pred == predicate and matches_pattern?(tuple, pattern)
          end)

        # Clean up temp storage
        if function_exported?(state.storage_mod, :terminate, 1) do
          state.storage_mod.terminate(temp_storage)
        end

        results

      {:error, reason} ->
        Logger.warning("Datalox: magic sets evaluation failed: #{inspect(reason)}")

        if function_exported?(state.storage_mod, :terminate, 1) do
          state.storage_mod.terminate(temp_storage)
        end

        []
    end
  end

  defp copy_base_facts(state, temp_storage) do
    mod = state.storage_mod

    predicates =
      if function_exported?(mod, :all_predicates, 1) do
        {:ok, preds} = mod.all_predicates(state.storage_state)
        preds
      else
        []
      end

    Enum.reduce(predicates, temp_storage, fn pred, st ->
      {:ok, facts} = mod.all(state.storage_state, pred)

      Enum.reduce(facts, st, fn tuple, st ->
        {:ok, st} = mod.insert(st, pred, tuple)
        st
      end)
    end)
  end

  defp incrementally_derive(state, predicate, tuple) do
    affected = Incremental.affected_rules(state.rules, predicate)

    if affected == [] do
      state
    else
      existing_facts = build_facts_map(state)

      new_facts =
        Enum.flat_map(affected, fn rule ->
          Incremental.compute_delta(rule, {predicate, tuple}, existing_facts)
        end)
        |> Enum.uniq()

      new_storage =
        Enum.reduce(new_facts, state.storage_state, fn {pred, args}, st ->
          {:ok, st} = state.storage_mod.insert(st, pred, args)
          st
        end)

      %{state | storage_state: new_storage}
    end
  end

  defp incrementally_retract(state, predicate, tuple) do
    affected = Incremental.affected_rules(state.rules, predicate)

    if affected == [] do
      state
    else
      existing_facts = build_facts_map(state)

      retractions =
        Enum.flat_map(affected, fn rule ->
          Incremental.compute_retractions(rule, {predicate, tuple}, existing_facts)
        end)
        |> Enum.uniq()

      new_storage =
        Enum.reduce(retractions, state.storage_state, fn {pred, args}, st ->
          {:ok, st} = state.storage_mod.delete(st, pred, args)
          st
        end)

      %{state | storage_state: new_storage}
    end
  end

  defp build_facts_map(state) do
    mod = state.storage_mod

    predicates =
      if function_exported?(mod, :all_predicates, 1) do
        case mod.all_predicates(state.storage_state) do
          {:ok, preds} -> preds
          _ -> []
        end
      else
        []
      end

    Enum.reduce(predicates, %{}, fn pred, acc ->
      case mod.all(state.storage_state, pred) do
        {:ok, facts} -> Map.put(acc, pred, facts)
        _ -> acc
      end
    end)
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

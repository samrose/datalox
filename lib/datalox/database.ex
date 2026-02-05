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

  alias Datalox.{Storage, Rule, Evaluator}

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
    # Record query metrics
    Datalox.Metrics.record_query(state.name, predicate)

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
  def handle_call({:load_rules, rules}, _from, state) do
    case Evaluator.evaluate(rules, state.storage_state, state.storage_mod) do
      {:ok, _derived, new_storage_state} ->
        new_state = %{state | rules: rules, storage_state: new_storage_state}
        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
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
end

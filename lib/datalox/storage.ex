defmodule Datalox.Storage do
  @moduledoc """
  Behaviour for Datalox storage backends.

  A storage backend is responsible for persisting and querying facts.
  The default implementation uses ETS tables.
  """

  @type state :: any()
  @type predicate :: atom()
  @type fact_tuple :: [any()]
  @type pattern :: [any()]

  @doc """
  Initialize the storage backend.
  """
  @callback init(opts :: keyword()) :: {:ok, state()} | {:error, term()}

  @doc """
  Insert a fact into storage.
  """
  @callback insert(state(), predicate(), fact_tuple()) :: {:ok, state()} | {:error, term()}

  @doc """
  Delete a fact from storage.
  """
  @callback delete(state(), predicate(), fact_tuple()) :: {:ok, state()} | {:error, term()}

  @doc """
  Lookup facts matching a pattern. Use :_ for wildcards.
  """
  @callback lookup(state(), predicate(), pattern()) :: {:ok, [fact_tuple()]} | {:error, term()}

  @doc """
  Return all facts for a predicate.
  """
  @callback all(state(), predicate()) :: {:ok, [fact_tuple()]} | {:error, term()}

  @doc """
  Return the number of facts stored for a predicate.
  """
  @callback count(state(), predicate()) :: {:ok, non_neg_integer()} | {:error, term()}

  @doc """
  Create a secondary index on a column for faster lookups.
  """
  @callback create_index(state(), predicate(), non_neg_integer()) ::
              {:ok, state()} | {:error, term()}

  @doc """
  Return a list of all predicates that have stored facts.
  """
  @callback all_predicates(state()) :: {:ok, [predicate()]}

  @doc """
  Clean up storage resources.
  """
  @callback terminate(state()) :: :ok

  @optional_callbacks terminate: 1, count: 2, create_index: 3, all_predicates: 1
end

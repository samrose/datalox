defmodule Datalox.Metrics do
  @moduledoc """
  Metrics collection for Datalox using Telemetry.

  Emits telemetry events and maintains ETS-based counters for:
  - `[:datalox, :query]` - fact queries
  - `[:datalox, :assert]` - fact assertions
  - `[:datalox, :retract]` - fact retractions
  - `[:datalox, :load_rules]` - rule loading
  """

  use GenServer

  @table_name :datalox_metrics

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(_opts) do
    :ets.new(@table_name, [:named_table, :public, :set])
    {:ok, %{}}
  end

  @doc """
  Records a query event.
  """
  @spec record_query(atom(), atom()) :: :ok
  def record_query(db_name, predicate) do
    increment_counter(db_name, :queries)
    increment_counter(db_name, {:predicate, predicate})

    :telemetry.execute(
      [:datalox, :query],
      %{count: 1},
      %{db: db_name, predicate: predicate}
    )

    :ok
  end

  @doc """
  Records an assertion event.
  """
  @spec record_assert(atom(), atom()) :: :ok
  def record_assert(db_name, predicate) do
    increment_counter(db_name, :assertions)

    :telemetry.execute(
      [:datalox, :assert],
      %{count: 1},
      %{db: db_name, predicate: predicate}
    )

    :ok
  end

  @doc """
  Records a retraction event.
  """
  @spec record_retract(atom(), atom()) :: :ok
  def record_retract(db_name, predicate) do
    increment_counter(db_name, :retractions)

    :telemetry.execute(
      [:datalox, :retract],
      %{count: 1},
      %{db: db_name, predicate: predicate}
    )

    :ok
  end

  @doc """
  Records a rule loading event.
  """
  @spec record_load_rules(atom(), non_neg_integer()) :: :ok
  def record_load_rules(db_name, rule_count) do
    :telemetry.execute(
      [:datalox, :load_rules],
      %{count: rule_count},
      %{db: db_name}
    )

    :ok
  end

  @doc """
  Gets stats for a database.
  """
  @spec get_stats(pid() | atom()) :: map()
  def get_stats(db) when is_pid(db) do
    case GenServer.call(db, :get_name) do
      {:ok, name} -> get_stats(name)
      _ -> get_stats(:unknown)
    end
  catch
    :exit, _ -> get_stats(:unknown)
  end

  def get_stats(db_name) do
    %{
      total_queries: lookup_counter(db_name, :queries),
      total_assertions: lookup_counter(db_name, :assertions),
      total_retractions: lookup_counter(db_name, :retractions),
      facts: %{base: 0, derived: 0}
    }
  end

  defp increment_counter(db_name, metric) do
    key = {db_name, metric}
    :ets.update_counter(@table_name, key, {2, 1}, {key, 0})
  end

  defp lookup_counter(db_name, metric) do
    case :ets.lookup(@table_name, {db_name, metric}) do
      [{_, count}] -> count
      [] -> 0
    end
  end
end

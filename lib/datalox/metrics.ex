defmodule Datalox.Metrics do
  @moduledoc """
  Metrics collection for Datalox using Telemetry.
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
    key = {db_name, :queries}
    :ets.update_counter(@table_name, key, {2, 1}, {key, 0})

    pred_key = {db_name, :predicate, predicate}
    :ets.update_counter(@table_name, pred_key, {2, 1}, {pred_key, 0})
    :ok
  end

  @doc """
  Gets stats for a database.
  """
  @spec get_stats(pid() | atom()) :: map()
  def get_stats(db) when is_pid(db) do
    # Try to get the name from the Database state
    case GenServer.call(db, :get_name) do
      {:ok, name} -> get_stats(name)
      _ -> get_stats(:unknown)
    end
  catch
    :exit, _ -> get_stats(:unknown)
  end

  def get_stats(db_name) do
    query_count =
      case :ets.lookup(@table_name, {db_name, :queries}) do
        [{_, count}] -> count
        [] -> 0
      end

    %{
      total_queries: query_count,
      facts: %{base: 0, derived: 0}
    }
  end
end

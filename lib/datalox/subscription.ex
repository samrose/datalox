defmodule Datalox.Subscription do
  @moduledoc """
  Subscription system for fact change notifications.
  """

  use GenServer

  defstruct subscriptions: []

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(_opts) do
    {:ok, %__MODULE__{}}
  end

  @doc """
  Subscribe to facts matching a pattern.
  """
  def subscribe(pid, pattern, callback) do
    GenServer.call(pid, {:subscribe, pattern, callback})
  end

  @doc """
  Notify subscribers of a fact change.
  """
  def notify(pid, event) do
    GenServer.cast(pid, {:notify, event})
  end

  @impl true
  def handle_call({:subscribe, pattern, callback}, _from, state) do
    subscription = {pattern, callback}
    new_state = %{state | subscriptions: [subscription | state.subscriptions]}
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_cast({:notify, {:assert, {pred, tuple} = fact}}, state) do
    Enum.each(state.subscriptions, fn {{sub_pred, sub_pattern}, callback} ->
      if pred == sub_pred and matches_pattern?(tuple, sub_pattern) do
        callback.(fact)
      end
    end)

    {:noreply, state}
  end

  def handle_cast({:notify, _event}, state) do
    {:noreply, state}
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

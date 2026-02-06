defmodule Datalox.Functions do
  @moduledoc "Registry for user-defined functions callable from rule bodies."

  use Agent

  def start_link(_opts), do: Agent.start_link(fn -> %{} end, name: __MODULE__)

  @doc "Register an Elixir function by name."
  @spec register(atom(), function()) :: :ok
  def register(name, fun) when is_atom(name) and is_function(fun) do
    Agent.update(__MODULE__, &Map.put(&1, name, fun))
  end

  @doc "Call a registered function with the given arguments."
  @spec call(atom(), [any()]) :: any()
  def call(name, args) do
    fun = Agent.get(__MODULE__, &Map.fetch!(&1, name))
    fun.(args)
  end

  @doc "Check if a function is registered."
  @spec registered?(atom()) :: boolean()
  def registered?(name) do
    Agent.get(__MODULE__, &Map.has_key?(&1, name))
  end
end

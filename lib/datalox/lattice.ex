defmodule Datalox.Lattice do
  @moduledoc "Behaviour for lattice types used in subsumptive evaluation."

  @callback bottom() :: any()
  @callback join(any(), any()) :: any()
  @callback leq?(any(), any()) :: boolean()

  defmodule Min do
    @moduledoc "Min lattice — keeps the smallest value."
    @behaviour Datalox.Lattice

    @impl true
    def bottom, do: :infinity

    @impl true
    def join(a, b), do: min(a, b)

    @impl true
    def leq?(a, b), do: a <= b
  end

  defmodule Max do
    @moduledoc "Max lattice — keeps the largest value."
    @behaviour Datalox.Lattice

    @impl true
    def bottom, do: :neg_infinity

    @impl true
    def join(a, b), do: max(a, b)

    @impl true
    def leq?(a, b), do: a >= b
  end

  defmodule Set do
    @moduledoc "Set lattice — union of sets."
    @behaviour Datalox.Lattice

    @impl true
    def bottom, do: MapSet.new()

    @impl true
    def join(a, b), do: MapSet.union(a, b)

    @impl true
    def leq?(a, b), do: MapSet.subset?(a, b)
  end
end

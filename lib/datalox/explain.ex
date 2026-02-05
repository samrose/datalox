defmodule Datalox.Explain do
  @moduledoc """
  Query explanation for Datalox.

  Provides derivation trees showing how facts were derived.
  """

  defstruct [:fact, :rule, :derivation]

  @type t :: %__MODULE__{
          fact: {atom(), list()},
          rule: atom() | nil,
          derivation: :base | [t()]
        }

  @doc """
  Creates an explanation for a base fact.
  """
  @spec base(tuple()) :: t()
  def base(fact) do
    %__MODULE__{fact: fact, rule: nil, derivation: :base}
  end

  @doc """
  Creates an explanation for a derived fact.
  """
  @spec derived(tuple(), atom(), [t()]) :: t()
  def derived(fact, rule, derivations) do
    %__MODULE__{fact: fact, rule: rule, derivation: derivations}
  end
end

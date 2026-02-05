defmodule Datalox.Rule do
  @moduledoc """
  Represents a Datalog rule.

  A rule consists of:
  - `head` - The derived predicate (consequent)
  - `body` - List of positive goals (antecedents)
  - `negations` - List of negated goals
  - `aggregations` - List of aggregate expressions
  - `guards` - List of comparison guards
  """

  @type variable :: atom()
  @type predicate :: atom()
  @type goal_term :: variable() | any()
  @type goal :: {predicate(), [goal_term()]}
  @type aggregation :: {:sum | :count | :min | :max | :avg, variable(), goal(), {atom(), any()}}

  @type t :: %__MODULE__{
          head: goal(),
          body: [goal()],
          negations: [goal()],
          aggregations: [aggregation()],
          guards: [any()]
        }

  @enforce_keys [:head, :body]
  defstruct head: nil,
            body: [],
            negations: [],
            aggregations: [],
            guards: []

  @doc """
  Creates a new rule.

  ## Examples

      iex> Rule.new({:ancestor, [:X, :Z]}, [{:parent, [:X, :Z]}])
      %Rule{head: {:ancestor, [:X, :Z]}, body: [{:parent, [:X, :Z]}], ...}

  """
  @spec new(goal(), [goal()], keyword()) :: t()
  def new(head, body, opts \\ []) do
    %__MODULE__{
      head: head,
      body: body,
      negations: Keyword.get(opts, :negations, []),
      aggregations: Keyword.get(opts, :aggregations, []),
      guards: Keyword.get(opts, :guards, [])
    }
  end

  @doc """
  Returns all variables in the rule (head, body, negations).
  """
  @spec variables(t()) :: MapSet.t(variable())
  def variables(%__MODULE__{} = rule) do
    head_vars = extract_variables(rule.head)
    body_vars = Enum.flat_map(rule.body, &extract_variables/1)
    neg_vars = Enum.flat_map(rule.negations, &extract_variables/1)

    MapSet.new(head_vars ++ body_vars ++ neg_vars)
  end

  @doc """
  Returns variables appearing in the rule head.
  """
  @spec head_variables(t()) :: MapSet.t(variable())
  def head_variables(%__MODULE__{head: head}) do
    head
    |> extract_variables()
    |> MapSet.new()
  end

  @doc """
  Returns the set of predicates this rule depends on.
  """
  @spec depends_on(t()) :: MapSet.t(predicate())
  def depends_on(%__MODULE__{body: body, negations: negations}) do
    body_preds = Enum.map(body, fn {pred, _} -> pred end)
    neg_preds = Enum.map(negations, fn {pred, _} -> pred end)

    MapSet.new(body_preds ++ neg_preds)
  end

  @doc """
  Returns true if the rule is recursive (head predicate appears in body).
  """
  @spec recursive?(t()) :: boolean()
  def recursive?(%__MODULE__{head: {head_pred, _}, body: body}) do
    Enum.any?(body, fn {pred, _} -> pred == head_pred end)
  end

  # Extract variables from a goal, filtering out wildcards (:_)
  defp extract_variables({_predicate, terms}) do
    terms
    |> Enum.filter(&variable?/1)
    |> Enum.reject(&(&1 == :_))
  end

  # A term is a variable if it's an uppercase atom
  defp variable?(term) when is_atom(term) do
    term
    |> Atom.to_string()
    |> String.first()
    |> String.match?(~r/^[A-Z_]$/)
  end

  defp variable?(_), do: false
end

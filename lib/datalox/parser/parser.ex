defmodule Datalox.Parser.Parser do
  @moduledoc """
  Parser for Datalog .dl files.

  Parses the token stream from the Lexer into facts and rules.
  """

  alias Datalox.Parser.Lexer
  alias Datalox.Rule

  @type parse_result :: {:fact, {atom(), list()}} | {:rule, Rule.t()}

  @doc """
  Parses a Datalog string into facts and rules.
  """
  @spec parse(String.t()) :: {:ok, [parse_result()]} | {:error, term()}
  def parse(input) do
    case Lexer.tokenize(input) do
      {:ok, tokens, "", _, _, _} ->
        parse_tokens(tokens, [])

      {:error, reason, _, _, _, _} ->
        {:error, {:lexer_error, reason}}
    end
  end

  @doc """
  Parses a .dl file into facts and rules.
  """
  @spec parse_file(String.t()) :: {:ok, [parse_result()]} | {:error, term()}
  def parse_file(path) do
    case File.read(path) do
      {:ok, content} -> parse(content)
      {:error, reason} -> {:error, {:file_error, reason}}
    end
  end

  # Parse token stream
  defp parse_tokens([], acc), do: {:ok, Enum.reverse(acc)}

  defp parse_tokens(tokens, acc) do
    case parse_statement(tokens) do
      {:ok, statement, rest} ->
        parse_tokens(rest, [statement | acc])

      {:error, _} = error ->
        error
    end
  end

  # Parse a single statement (fact or rule)
  defp parse_statement([{:atom, pred} | rest]) do
    case parse_goal({:atom, pred}, rest) do
      {:ok, head, [:implies | body_tokens]} ->
        # It's a rule
        case parse_body(body_tokens) do
          {:ok, body, negations, [:dot | rest]} ->
            {head_pred, head_terms} = head
            converted_head = {head_pred, Enum.map(head_terms, &term_to_rule_term/1)}
            rule = Rule.new(converted_head, body, negations: negations)
            {:ok, {:rule, rule}, rest}

          {:error, _} = error ->
            error
        end

      {:ok, goal, [:dot | rest]} ->
        # It's a fact
        {pred, args} = goal
        fact_args = Enum.map(args, &term_to_value/1)
        {:ok, {:fact, {pred, fact_args}}, rest}

      {:error, _} = error ->
        error
    end
  end

  defp parse_statement(tokens) do
    {:error, {:unexpected_token, tokens}}
  end

  # Parse a goal: predicate(arg1, arg2, ...)
  defp parse_goal({:atom, pred}, [:lparen | rest]) do
    case parse_args(rest, []) do
      {:ok, args, [:rparen | rest]} ->
        {:ok, {String.to_atom(pred), args}, rest}

      {:error, _} = error ->
        error
    end
  end

  # Parse argument list
  defp parse_args([:rparen | _] = tokens, acc) do
    {:ok, Enum.reverse(acc), tokens}
  end

  defp parse_args(tokens, acc) do
    case parse_term(tokens) do
      {:ok, term, [:comma | rest]} ->
        parse_args(rest, [term | acc])

      {:ok, term, rest} ->
        {:ok, Enum.reverse([term | acc]), rest}

      {:error, _} = error ->
        error
    end
  end

  # Parse a term (variable, atom, string, integer, wildcard)
  defp parse_term([{:var, name} | rest]) do
    {:ok, {:var, String.to_atom(name)}, rest}
  end

  defp parse_term([{:atom, name} | rest]) do
    {:ok, {:atom, String.to_atom(name)}, rest}
  end

  defp parse_term([{:string, value} | rest]) do
    {:ok, {:string, value}, rest}
  end

  defp parse_term([{:integer, value} | rest]) do
    {:ok, {:integer, value}, rest}
  end

  defp parse_term([:wildcard | rest]) do
    {:ok, {:wildcard}, rest}
  end

  defp parse_term(tokens) do
    {:error, {:unexpected_term, tokens}}
  end

  # Parse rule body
  defp parse_body(tokens) do
    parse_body_goals(tokens, [], [])
  end

  defp parse_body_goals([:not, {:atom, pred} | rest], goals, negations) do
    case parse_goal({:atom, pred}, rest) do
      {:ok, goal, [:comma | rest]} ->
        {neg_pred, neg_terms} = goal
        neg_goal = {neg_pred, Enum.map(neg_terms, &term_to_rule_term/1)}
        parse_body_goals(rest, goals, [neg_goal | negations])

      {:ok, goal, rest} ->
        {neg_pred, neg_terms} = goal
        neg_goal = {neg_pred, Enum.map(neg_terms, &term_to_rule_term/1)}
        {:ok, Enum.reverse(goals), Enum.reverse([neg_goal | negations]), rest}

      {:error, _} = error ->
        error
    end
  end

  defp parse_body_goals([{:atom, pred} | rest], goals, negations) do
    case parse_goal({:atom, pred}, rest) do
      {:ok, goal, [:comma | rest]} ->
        {pred, terms} = goal
        converted_goal = {pred, Enum.map(terms, &term_to_rule_term/1)}
        parse_body_goals(rest, [converted_goal | goals], negations)

      {:ok, goal, rest} ->
        {pred, terms} = goal
        converted_goal = {pred, Enum.map(terms, &term_to_rule_term/1)}
        {:ok, Enum.reverse([converted_goal | goals]), Enum.reverse(negations), rest}

      {:error, _} = error ->
        error
    end
  end

  # Convert parsed term to rule term (variable atom)
  defp term_to_rule_term({:var, name}), do: name
  defp term_to_rule_term({:atom, name}), do: name
  defp term_to_rule_term({:string, value}), do: value
  defp term_to_rule_term({:integer, value}), do: value
  defp term_to_rule_term({:wildcard}), do: :_

  # Convert parsed term to fact value
  defp term_to_value({:var, name}), do: name
  defp term_to_value({:atom, name}), do: name
  defp term_to_value({:string, value}), do: value
  defp term_to_value({:integer, value}), do: value
  defp term_to_value({:wildcard}), do: :_
end

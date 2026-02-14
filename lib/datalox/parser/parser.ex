defmodule Datalox.Parser.Parser do
  @moduledoc """
  Parser for Datalog .dl files.

  Parses the token stream from the Lexer into facts and rules.
  """

  alias Datalox.Parser.Lexer
  alias Datalox.Rule

  @type parse_result :: {:fact, {atom(), list()}} | {:rule, Rule.t()}

  @max_atoms 10_000

  @doc """
  Parses a Datalog string into facts and rules.

  ## Options

    * `:max_atoms` - Maximum number of unique atoms/variables allowed.
      Defaults to #{@max_atoms}. Protects against atom table exhaustion
      from untrusted input.

  """
  @spec parse(String.t(), keyword()) :: {:ok, [parse_result()]} | {:error, term()}
  def parse(input, opts \\ []) do
    max_atoms = Keyword.get(opts, :max_atoms, @max_atoms)

    case Lexer.tokenize(input) do
      {:ok, tokens, "", _, _, _} ->
        atom_count = count_unique_atoms(tokens)

        if atom_count > max_atoms do
          {:error, {:too_many_atoms, atom_count, max_atoms}}
        else
          parse_tokens(tokens, [])
        end

      {:error, reason, _, _, _, _} ->
        {:error, {:lexer_error, reason}}
    end
  end

  @doc """
  Parses a .dl file into facts and rules.

  Accepts the same options as `parse/2`.
  """
  @spec parse_file(String.t(), keyword()) :: {:ok, [parse_result()]} | {:error, term()}
  def parse_file(path, opts \\ []) do
    case File.read(path) do
      {:ok, content} -> parse(content, opts)
      {:error, reason} -> {:error, {:file_error, reason}}
    end
  end

  defp count_unique_atoms(tokens) do
    tokens
    |> Enum.flat_map(fn
      {:atom, name} -> [name]
      {:var, name} -> [name]
      _ -> []
    end)
    |> Enum.uniq()
    |> length()
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
          {:ok, body, negations, aggregations, guards, [:dot | rest]} ->
            {head_pred, head_terms} = head
            converted_head = {head_pred, Enum.map(head_terms, &term_to_rule_term/1)}

            rule =
              Rule.new(converted_head, body,
                negations: negations,
                aggregations: aggregations,
                guards: guards
              )

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

  defp parse_term([{:float, value} | rest]) do
    {:ok, {:float, value}, rest}
  end

  defp parse_term([:wildcard | rest]) do
    {:ok, {:wildcard}, rest}
  end

  defp parse_term(tokens) do
    {:error, {:unexpected_term, tokens}}
  end

  # Parse rule body
  defp parse_body(tokens) do
    parse_body_goals(tokens, [], [], [], [])
  end

  # Aggregation: Var = agg_fn(args)
  defp parse_body_goals(
         [{:var, var_name}, :equals, {:atom, agg_name} | rest],
         goals,
         negations,
         aggs,
         guards
       )
       when agg_name in ~w(count sum min max avg collect) do
    case parse_goal({:atom, agg_name}, rest) do
      {:ok, {_pred, agg_args}, [:comma | rest]} ->
        agg = build_aggregation(agg_name, var_name, agg_args)
        parse_body_goals(rest, goals, negations, [agg | aggs], guards)

      {:ok, {_pred, agg_args}, rest} ->
        agg = build_aggregation(agg_name, var_name, agg_args)

        {:ok, Enum.reverse(goals), Enum.reverse(negations), Enum.reverse([agg | aggs]),
         Enum.reverse(guards), rest}

      {:error, _} = error ->
        error
    end
  end

  defp parse_body_goals([:not, {:atom, pred} | rest], goals, negations, aggs, guards) do
    case parse_goal({:atom, pred}, rest) do
      {:ok, goal, [:comma | rest]} ->
        {neg_pred, neg_terms} = goal
        neg_goal = {neg_pred, Enum.map(neg_terms, &term_to_rule_term/1)}
        parse_body_goals(rest, goals, [neg_goal | negations], aggs, guards)

      {:ok, goal, rest} ->
        {neg_pred, neg_terms} = goal
        neg_goal = {neg_pred, Enum.map(neg_terms, &term_to_rule_term/1)}

        {:ok, Enum.reverse(goals), Enum.reverse([neg_goal | negations]), Enum.reverse(aggs),
         Enum.reverse(guards), rest}

      {:error, _} = error ->
        error
    end
  end

  defp parse_body_goals([{:atom, pred} | rest], goals, negations, aggs, guards) do
    case parse_goal({:atom, pred}, rest) do
      {:ok, goal, [:comma | rest]} ->
        {pred, terms} = goal
        converted_goal = {pred, Enum.map(terms, &term_to_rule_term/1)}
        parse_body_goals(rest, [converted_goal | goals], negations, aggs, guards)

      {:ok, goal, rest} ->
        {pred, terms} = goal
        converted_goal = {pred, Enum.map(terms, &term_to_rule_term/1)}

        {:ok, Enum.reverse([converted_goal | goals]), Enum.reverse(negations), Enum.reverse(aggs),
         Enum.reverse(guards), rest}

      {:error, _} = error ->
        error
    end
  end

  # Guard expression (comparison or assignment) — catch-all for vars, numbers, parens
  defp parse_body_goals(tokens, goals, negations, aggs, guards) do
    case parse_guard(tokens) do
      {:ok, guard, [:comma | rest]} ->
        parse_body_goals(rest, goals, negations, aggs, [guard | guards])

      {:ok, guard, rest} ->
        {:ok, Enum.reverse(goals), Enum.reverse(negations), Enum.reverse(aggs),
         Enum.reverse([guard | guards]), rest}

      {:error, _} = error ->
        error
    end
  end

  # Parse a guard: comparison (Expr Op Expr) or assignment (Var = Expr)
  defp parse_guard(tokens) do
    case parse_arith_expr(tokens) do
      {:ok, lhs, [:equals | rest]} ->
        case parse_arith_expr(rest) do
          {:ok, rhs, rest} -> {:ok, {:=, lhs, rhs}, rest}
          error -> error
        end

      {:ok, lhs, [op | rest]} when op in [:gt, :lt, :gte, :lte, :neq] ->
        case parse_arith_expr(rest) do
          {:ok, rhs, rest} -> {:ok, {comparison_op(op), lhs, rhs}, rest}
          error -> error
        end

      {:ok, _expr, rest} ->
        {:error, {:expected_operator, rest}}

      error ->
        error
    end
  end

  defp comparison_op(:gt), do: :>
  defp comparison_op(:lt), do: :<
  defp comparison_op(:gte), do: :>=
  defp comparison_op(:lte), do: :<=
  defp comparison_op(:neq), do: :!=

  # Arithmetic expression parsing with standard precedence
  defp parse_arith_expr(tokens), do: parse_add_expr(tokens)

  # Addition / subtraction (lowest arithmetic precedence)
  defp parse_add_expr(tokens) do
    case parse_mul_expr(tokens) do
      {:ok, left, rest} -> parse_add_rest(left, rest)
      error -> error
    end
  end

  defp parse_add_rest(left, [:plus | rest]) do
    case parse_mul_expr(rest) do
      {:ok, right, rest} -> parse_add_rest({:+, left, right}, rest)
      error -> error
    end
  end

  defp parse_add_rest(left, [:minus | rest]) do
    case parse_mul_expr(rest) do
      {:ok, right, rest} -> parse_add_rest({:-, left, right}, rest)
      error -> error
    end
  end

  defp parse_add_rest(left, rest), do: {:ok, left, rest}

  # Multiplication / division (higher arithmetic precedence)
  defp parse_mul_expr(tokens) do
    case parse_primary_expr(tokens) do
      {:ok, left, rest} -> parse_mul_rest(left, rest)
      error -> error
    end
  end

  defp parse_mul_rest(left, [:star | rest]) do
    case parse_primary_expr(rest) do
      {:ok, right, rest} -> parse_mul_rest({:*, left, right}, rest)
      error -> error
    end
  end

  defp parse_mul_rest(left, [:slash | rest]) do
    case parse_primary_expr(rest) do
      {:ok, right, rest} -> parse_mul_rest({:/, left, right}, rest)
      error -> error
    end
  end

  defp parse_mul_rest(left, rest), do: {:ok, left, rest}

  # Primary expressions: variables, numbers, atoms, strings, parenthesized exprs
  defp parse_primary_expr([{:var, name} | rest]), do: {:ok, String.to_atom(name), rest}
  defp parse_primary_expr([{:integer, value} | rest]), do: {:ok, value, rest}
  defp parse_primary_expr([{:float, value} | rest]), do: {:ok, value, rest}
  defp parse_primary_expr([{:atom, name} | rest]), do: {:ok, String.to_atom(name), rest}
  defp parse_primary_expr([{:string, value} | rest]), do: {:ok, value, rest}

  defp parse_primary_expr([:lparen | rest]) do
    case parse_arith_expr(rest) do
      {:ok, expr, [:rparen | rest]} -> {:ok, expr, rest}
      {:ok, _, _} -> {:error, :expected_rparen}
      error -> error
    end
  end

  defp parse_primary_expr(tokens), do: {:error, {:unexpected_expr_token, tokens}}

  defp build_aggregation(agg_name, var_name, agg_args) do
    agg_fn = String.to_atom(agg_name)
    target_var = String.to_atom(var_name)
    rule_args = Enum.map(agg_args, &term_to_rule_term/1)

    source_var =
      case {agg_fn, rule_args} do
        {:count, _} -> :_
        {_, [src | _]} -> src
        _ -> :_
      end

    group_vars = rule_args

    {agg_fn, target_var, source_var, group_vars}
  end

  # Convert parsed term to rule term (variable atom)
  defp term_to_rule_term({:var, name}), do: name
  defp term_to_rule_term({:atom, name}), do: name
  defp term_to_rule_term({:string, value}), do: value
  defp term_to_rule_term({:integer, value}), do: value
  defp term_to_rule_term({:float, value}), do: value
  defp term_to_rule_term({:wildcard}), do: :_

  # Convert parsed term to fact value
  defp term_to_value({:var, name}), do: name
  defp term_to_value({:atom, name}), do: name
  defp term_to_value({:string, value}), do: value
  defp term_to_value({:integer, value}), do: value
  defp term_to_value({:float, value}), do: value
  defp term_to_value({:wildcard}), do: :_
end

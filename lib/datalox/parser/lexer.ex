defmodule Datalox.Parser.Lexer do
  @moduledoc """
  Lexer for Datalog .dl files using NimbleParsec.
  """

  import NimbleParsec

  # Whitespace (must consume at least 1 character)
  whitespace = ascii_string([?\s, ?\t, ?\r, ?\n], min: 1) |> ignore()

  # Comments (% to end of line)
  comment = string("%") |> utf8_string([not: ?\n], min: 0) |> optional(string("\n")) |> ignore()

  # Atoms (lowercase identifiers)
  atom_chars = ascii_string([?a..?z, ?A..?Z, ?0..?9, ?_], min: 0)

  atom_token =
    ascii_char([?a..?z])
    |> concat(atom_chars)
    |> reduce({List, :to_string, []})
    |> unwrap_and_tag(:atom)

  # Variables (uppercase identifiers or underscore followed by more chars)
  var_token =
    ascii_char([?A..?Z])
    |> concat(atom_chars)
    |> reduce({List, :to_string, []})
    |> unwrap_and_tag(:var)

  # Wildcard (standalone underscore)
  wildcard_token =
    string("_")
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))
    |> replace(:wildcard)

  # Underscore variable (_Name)
  underscore_var_token =
    ascii_char([?_])
    |> concat(ascii_string([?a..?z, ?A..?Z, ?0..?9, ?_], min: 1))
    |> reduce({List, :to_string, []})
    |> unwrap_and_tag(:var)

  # Strings
  string_token =
    ignore(string("\""))
    |> utf8_string([not: ?"], min: 0)
    |> ignore(string("\""))
    |> unwrap_and_tag(:string)

  # Floats (must come before integer in choice, e.g. 1.5 vs 1 then .)
  float_token =
    optional(string("-"))
    |> ascii_string([?0..?9], min: 1)
    |> string(".")
    |> ascii_string([?0..?9], min: 1)
    |> reduce({Enum, :join, []})
    |> map({String, :to_float, []})
    |> unwrap_and_tag(:float)

  # Integers
  integer_token =
    optional(string("-"))
    |> ascii_string([?0..?9], min: 1)
    |> reduce({Enum, :join, []})
    |> map({String, :to_integer, []})
    |> unwrap_and_tag(:integer)

  # Keywords (must not be followed by identifier chars)
  not_keyword =
    string("not")
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))
    |> replace(:not)

  # Punctuation
  lparen = string("(") |> replace(:lparen)
  rparen = string(")") |> replace(:rparen)
  comma = string(",") |> replace(:comma)
  dot = string(".") |> replace(:dot)
  implies = string(":-") |> replace(:implies)

  # Comparison operators (multi-char before single-char)
  gte = string(">=") |> replace(:gte)
  lte = string("<=") |> replace(:lte)
  neq = string("!=") |> replace(:neq)
  gt = string(">") |> replace(:gt)
  lt = string("<") |> replace(:lt)
  equals = string("=") |> replace(:equals)

  # Arithmetic operators
  plus = string("+") |> replace(:plus)
  star = string("*") |> replace(:star)
  slash = string("/") |> replace(:slash)
  minus = string("-") |> replace(:minus)

  # Token - order matters! Keywords before atoms, longer punctuation before shorter
  # Multi-char operators before single-char; float before integer; minus after integer
  token =
    choice([
      whitespace,
      comment,
      not_keyword,
      implies,
      # Comparison operators (multi-char before single-char, before equals)
      gte,
      lte,
      neq,
      gt,
      lt,
      equals,
      # Arithmetic operators (minus after numbers so -5 is integer, not minus + 5)
      plus,
      star,
      slash,
      lparen,
      rparen,
      comma,
      # Numbers: float before integer (1.5 vs 1 + dot); both before minus and dot
      float_token,
      string_token,
      integer_token,
      minus,
      dot,
      wildcard_token,
      underscore_var_token,
      var_token,
      atom_token
    ])

  defparsec(:tokenize, repeat(token) |> eos())
end

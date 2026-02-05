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

  # Token - order matters! Keywords before atoms, longer punctuation before shorter
  token = choice([
    whitespace,
    comment,
    not_keyword,
    implies,
    lparen,
    rparen,
    comma,
    dot,
    string_token,
    integer_token,
    wildcard_token,
    underscore_var_token,
    var_token,
    atom_token
  ])

  defparsec :tokenize, repeat(token) |> eos()
end

defmodule Datalox.Parser.LexerTest do
  use ExUnit.Case, async: true

  alias Datalox.Parser.Lexer

  describe "tokenize/1" do
    test "tokenizes a simple fact" do
      input = ~s|user("alice", admin).|
      {:ok, tokens, "", _, _, _} = Lexer.tokenize(input)

      assert [
               {:atom, "user"},
               :lparen,
               {:string, "alice"},
               :comma,
               {:atom, "admin"},
               :rparen,
               :dot
             ] = tokens
    end

    test "tokenizes a rule" do
      input = ~s|ancestor(X, Y) :- parent(X, Y).|
      {:ok, tokens, "", _, _, _} = Lexer.tokenize(input)

      assert [
               {:atom, "ancestor"},
               :lparen,
               {:var, "X"},
               :comma,
               {:var, "Y"},
               :rparen,
               :implies,
               {:atom, "parent"},
               :lparen,
               {:var, "X"},
               :comma,
               {:var, "Y"},
               :rparen,
               :dot
             ] = tokens
    end

    test "tokenizes negation" do
      input = ~s|active(X) :- user(X), not banned(X).|
      {:ok, tokens, "", _, _, _} = Lexer.tokenize(input)

      assert :not in tokens
    end

    test "handles comments" do
      input = """
      % This is a comment
      user("alice", admin).
      """

      {:ok, tokens, "", _, _, _} = Lexer.tokenize(input)

      # Comment should be skipped
      assert {:atom, "user"} in tokens
    end

    test "tokenizes numbers" do
      input = ~s|age("alice", 30).|
      {:ok, tokens, "", _, _, _} = Lexer.tokenize(input)

      assert {:integer, 30} in tokens
    end
  end

  describe "aggregation tokens" do
    test "tokenizes = sign" do
      {:ok, tokens, _, _, _, _} = Lexer.tokenize("N = count")
      assert tokens == [{:var, "N"}, :equals, {:atom, "count"}]
    end

    test "tokenizes full aggregation expression" do
      {:ok, tokens, _, _, _, _} = Lexer.tokenize("N = sum(Amount)")
      assert tokens == [{:var, "N"}, :equals, {:atom, "sum"}, :lparen, {:var, "Amount"}, :rparen]
    end
  end
end
